using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AvtoBus.RabbitMq;

/// <summary>
/// RabbitMQ-транспорт (идеи 61–62): quorum queues, publisher confirms, автотопология, DLQ.
///
/// Семантика:
/// — Queue destination — отдельная durable quorum-очередь; консьюмеры одной группы делят
///   сообщения (work queue), разные группы — нет.
/// — Topic destination — durable stream-очередь (лог с retention, как Kafka): публикация не
///   теряется до появления консьюмеров, каждая группа читает лог независимо и получает копии.
/// — Publisher confirms: SendAsync не завершается, пока брокер не подтвердил запись на диск.
/// — Back-pressure — через BasicQos (prefetch) и bounded-буфер внутри ReceiveAsync.
/// — Reject(requeue) = пере-публикация с инкрементом DeliveryAttempt (транспорт сам ведёт
///   счётчик попыток, не завися от версии брокера); после <c>DeliveryLimit</c> попыток — в DLQ.
///   Reject(без requeue) = BasicNack(requeue: false) → в DLQ (если включён).
/// </summary>
public sealed class RabbitMqTransport : ITransport, IDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly IConnection _connection;
    private readonly IChannel _publishChannel;
    private readonly PublisherConfirmationTracker _confirmTracker;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private int _disposed;

    public RabbitMqTransport(RabbitMqOptions options)
    {
        _options = options;

        var factory = new ConnectionFactory
        {
            Uri = new Uri(options.ConnectionString),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = options.NetworkRecoveryInterval,
            RequestedHeartbeat = options.RequestedHeartbeat,
            ClientProvidedName = options.ClientProvidedName,
        };

        _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        _publishChannel = _connection
            .CreateChannelAsync(new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: false,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: null))
            .GetAwaiter().GetResult();
        _confirmTracker = new PublisherConfirmationTracker(_publishChannel);
    }

    public string Name => "rabbitmq";

    public async ValueTask SendAsync(
        Envelope envelope,
        TransportDestination destination,
        CancellationToken ct = default)
    {
        await _publishLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await DeclareSendTopologyAsync(_publishChannel, destination, ct).ConfigureAwait(false);
            await PublishCoreAsync(envelope, routingKey: destination.Name, ct).ConfigureAwait(false);
        }
        finally
        {
            _publishLock.Release();
        }
    }

    /// <summary>
    /// Один консьюмер-канал на подписку: топология (очередь/топик), QoS, bounded-буфер.
    /// Сообщение считается «в работе», пока не подтверждено: если обработчик упал без ack/nack,
    /// брокер вернёт его после закрытия канала.
    /// </summary>
    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var destination = subscription.Destination;
        var queueName = destination.Name;

        var channel = await _connection.CreateChannelAsync().ConfigureAwait(false);
        try
        {
            await DeclareConsumeTopologyAsync(channel, destination, ct).ConfigureAwait(false);
            await channel.BasicQosAsync(
                0,
                (ushort)Math.Clamp(subscription.PrefetchCount, 1, ushort.MaxValue),
                global: false,
                ct).ConfigureAwait(false);

            var outbox = Channel.CreateBounded<ITransportMessage>(
                new BoundedChannelOptions(Math.Max(1, subscription.PrefetchCount))
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                });

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, args) => DeliverAsync(channel, args, outbox.Writer, this, queueName, ct);

            consumer.ShutdownAsync += (_, _) =>
            {
                outbox.Writer.TryComplete();
                return Task.CompletedTask;
            };

            // Stream-очереди (топики): консьюмер читает лог с начала — новая группа получает
            // все сохранённые сообщения, как Kafka с auto.offset.reset=earliest.
            IDictionary<string, object?>? consumerArguments = destination.Kind == DestinationKind.Topic
                ? new Dictionary<string, object?> { ["x-stream-offset"] = "first" }
                : null;

            await channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: false,
                consumerTag: "avb-" + Guid.NewGuid().ToString("N"),
                noLocal: false,
                exclusive: false,
                arguments: consumerArguments,
                consumer: consumer,
                cancellationToken: ct).ConfigureAwait(false);

            try
            {
                await foreach (var message in outbox.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    yield return message;
            }
            finally
            {
                outbox.Writer.TryComplete();
            }
        }
        finally
        {
            try
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Канал уже закрыт брокером (recovery) — подписка завершилась, это ок.
            }

            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Идемпотентно создаёт топологию: для queue — очередь (+DLQ), для topic — stream-очередь.
    /// Вызывается один раз при старте, до подъёма консьюмеров (идея 55).
    /// </summary>
    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        var channel = await _connection.CreateChannelAsync().ConfigureAwait(false);
        try
        {
            foreach (var destination in destinations)
            {
                if (destination.Kind == DestinationKind.Queue)
                    await EnsureQueueAsync(channel, destination.Name, ct).ConfigureAwait(false);
                else
                    await EnsureStreamAsync(channel, destination.Name, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            try
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Не критично: топология уже применена, канал закрывается при recovery.
            }

            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _confirmTracker.Dispose();

        try
        {
            _publishChannel.CloseAsync().GetAwaiter().GetResult();
        }
        catch
        {
            // Канал уже закрыт.
        }

        _publishChannel.Dispose();

        try
        {
            _connection.CloseAsync().GetAwaiter().GetResult();
        }
        catch
        {
            // Соединение уже закрыто.
        }

        _connection.Dispose();
        _publishLock.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Публикация ──

    /// <summary>Публикация с publisher confirm: номер последовательности → ожидание BasicAck.</summary>
    private async Task PublishCoreAsync(Envelope envelope, string routingKey, CancellationToken ct)
    {
        var (properties, body) = RabbitMqEnvelopeSerializer.ToRabbitMq(envelope);

        var sequence = await _publishChannel.GetNextPublishSequenceNumberAsync(ct).ConfigureAwait(false);
        var confirmation = _confirmTracker.WaitForConfirmationAsync(sequence, _options.PublishConfirmTimeout, ct);

        try
        {
            await _publishChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey,
                mandatory: true,
                properties,
                body,
                ct).ConfigureAwait(false);
        }
        catch
        {
            _confirmTracker.Unregister(sequence);
            throw;
        }

        try
        {
            await confirmation.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"RabbitMQ не подтвердил публикацию в '{routingKey}' за {_options.PublishConfirmTimeout}.");
        }
    }

    /// <summary>
    /// Reject(requeue) реализуется как пере-публикация копии с инкрементом <see cref="Envelope.DeliveryAttempt"/>:
    /// счётчик попыток не зависит от версии брокера (4.3+ не ставит x-delivery-count на basic.nack).
    /// </summary>
    private async ValueTask RequeueAsync(Envelope envelope, string queueName, CancellationToken ct)
    {
        await _publishLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await PublishCoreAsync(envelope.NextAttempt(), queueName, ct).ConfigureAwait(false);
        }
        finally
        {
            _publishLock.Release();
        }
    }

    // ── Топология ──

    private async Task DeclareSendTopologyAsync(IChannel channel, TransportDestination destination, CancellationToken ct)
    {
        if (destination.Kind == DestinationKind.Queue)
            await EnsureQueueAsync(channel, destination.Name, ct).ConfigureAwait(false);
        else
            await EnsureStreamAsync(channel, destination.Name, ct).ConfigureAwait(false);
    }

    private async Task DeclareConsumeTopologyAsync(IChannel channel, TransportDestination destination, CancellationToken ct)
    {
        if (destination.Kind == DestinationKind.Queue)
            await EnsureQueueAsync(channel, destination.Name, ct).ConfigureAwait(false);
        else
            await EnsureStreamAsync(channel, destination.Name, ct).ConfigureAwait(false);
    }

    private async Task EnsureQueueAsync(IChannel channel, string queueName, CancellationToken ct)
    {
        if (_options.UseDeadLetterExchange)
        {
            // DLQ: exchange {queue}.dlx → очередь {queue}.dlq по routing key = имени очереди.
            var dlx = queueName + ".dlx";
            var dlq = queueName + ".dlq";

            await channel.ExchangeDeclareAsync(
                dlx, "direct", durable: true, autoDelete: false, arguments: null,
                passive: false, noWait: false, ct).ConfigureAwait(false);
            await DeclareWorkQueueAsync(channel, dlq, hasDlx: false, ct).ConfigureAwait(false);
            await channel.QueueBindAsync(dlq, dlx, queueName, arguments: null, noWait: false, ct).ConfigureAwait(false);
        }

        await DeclareWorkQueueAsync(channel, queueName, hasDlx: true, ct).ConfigureAwait(false);
    }

    private async Task DeclareWorkQueueAsync(IChannel channel, string queueName, bool hasDlx, CancellationToken ct)
    {
        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = _options.UseQuorumQueues ? "quorum" : "classic",
        };

        if (_options.UseQuorumQueues)
            arguments["x-delivery-limit"] = _options.DeliveryLimit;

        if (hasDlx && _options.UseDeadLetterExchange)
        {
            arguments["x-dead-letter-exchange"] = queueName + ".dlx";
            arguments["x-dead-letter-routing-key"] = queueName;
        }

        await channel.QueueDeclareAsync(
            queueName, durable: true, exclusive: false, autoDelete: false,
            arguments: arguments, passive: false, noWait: false, ct).ConfigureAwait(false);
    }

    private async Task EnsureStreamAsync(IChannel channel, string streamName, CancellationToken ct)
    {
        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "stream",
            ["x-max-length-bytes"] = _options.TopicRetentionMaxBytes,
        };

        await channel.QueueDeclareAsync(
            streamName, durable: true, exclusive: false, autoDelete: false,
            arguments: arguments, passive: false, noWait: false, ct).ConfigureAwait(false);
    }

    // ── Доставка ──

    private static async Task DeliverAsync(
        IChannel channel,
        BasicDeliverEventArgs args,
        ChannelWriter<ITransportMessage> outbox,
        RabbitMqTransport transport,
        string queueName,
        CancellationToken ct)
    {
        try
        {
            var envelope = RabbitMqEnvelopeSerializer.FromRabbitMq(args);
            await outbox.WriteAsync(new RabbitMqMessage(transport, channel, args.DeliveryTag, envelope, queueName), ct).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            // Несовместимый продюсер — терминальный nack без requeue, чтобы не зациклиться на мусоре.
            await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false, CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Подписка завершается — сообщение остаётся в очереди, вернётся следующему консьюмеру.
        }
        catch (System.Threading.Channels.ChannelClosedException)
        {
            // Буфер уже закрыт на остановке подписки.
        }
        catch (Exception) when (channel.IsClosed || ct.IsCancellationRequested)
        {
            // Канал закрыт во время доставки — ack невозможен, сообщение пере-доставится.
        }
    }

    /// <summary>
    /// Сообщение из AMQP. Ack = BasicAck; Reject(requeue) = пере-публикация с инкрементом попытки
    /// (до <see cref="RabbitMqOptions.DeliveryLimit"/>, дальше — DLQ); Reject(без requeue) =
    /// BasicNack(requeue: false) → в DLQ или в никуда.
    /// </summary>
    private sealed class RabbitMqMessage : ITransportMessage
    {
        private readonly RabbitMqTransport _transport;
        private readonly IChannel _channel;
        private readonly ulong _deliveryTag;
        private readonly string _queueName;
        private int _settled;

        public RabbitMqMessage(
            RabbitMqTransport transport, IChannel channel, ulong deliveryTag, Envelope envelope, string queueName)
        {
            _transport = transport;
            _channel = channel;
            _deliveryTag = deliveryTag;
            _queueName = queueName;
            Envelope = envelope;
            Source = TransportDestination.Queue(queueName);
        }

        public Envelope Envelope { get; }

        public TransportDestination Source { get; }

        public ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return ValueTask.CompletedTask;

            return _channel.BasicAckAsync(_deliveryTag, multiple: false, ct);
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue && Envelope.DeliveryAttempt < _transport._options.DeliveryLimit)
            {
                // Сначала публикуем копию, потом снимаем оригинал: при сбое публикации оригинал
                // останется в очереди (at-least-once, дубликаты дедуплицируются по MessageId).
                await _transport.RequeueAsync(Envelope, _queueName, ct).ConfigureAwait(false);
                await _channel.BasicAckAsync(_deliveryTag, multiple: false, ct).ConfigureAwait(false);
            }
            else
            {
                await _channel.BasicNackAsync(_deliveryTag, multiple: false, requeue: false, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Отслеживание publisher confirms на публикующем канале. Публикации сериализованы
    /// (<see cref="_publishLock"/>), поэтому одновременно «в полёте» одна: номер последовательности
    /// из <see cref="IChannel.GetNextPublishSequenceNumberAsync"/> связывается с подтверждением из
    /// событий BasicAcks/BasicNacks (включая batch-подтверждения через Multiple).
    /// </summary>
    private sealed class PublisherConfirmationTracker : IDisposable
    {
        private readonly IChannel _channel;
        private readonly object _gate = new();
        private readonly Dictionary<ulong, TaskCompletionSource> _pending = new();
        private bool _failed;

        public PublisherConfirmationTracker(IChannel channel)
        {
            _channel = channel;
            _channel.BasicAcksAsync += OnAckAsync;
            _channel.BasicNacksAsync += OnNackAsync;
            _channel.ChannelShutdownAsync += OnShutdownAsync;
        }

        /// <summary>Регистрирует ожидание подтверждения до публикации и возвращает задачу ожидания.</summary>
        public Task WaitForConfirmationAsync(ulong sequence, TimeSpan timeout, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                if (_failed)
                {
                    tcs.SetException(new InvalidOperationException("Канал публикации закрыт."));
                }
                else
                {
                    _pending[sequence] = tcs;
                }
            }

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(timeout);
            return tcs.Task.WaitAsync(timeoutCts.Token);
        }

        /// <summary>Снимает ожидание, если публикация упала до отправки (подтверждение уже не придёт).</summary>
        public void Unregister(ulong sequence)
        {
            lock (_gate)
            {
                _pending.Remove(sequence);
            }
        }

        private Task OnAckAsync(object? sender, BasicAckEventArgs args)
        {
            CompleteUpTo(args.DeliveryTag, confirmed: true);
            return Task.CompletedTask;
        }

        private Task OnNackAsync(object? sender, BasicNackEventArgs args)
        {
            CompleteUpTo(args.DeliveryTag, confirmed: false);
            return Task.CompletedTask;
        }

        private Task OnShutdownAsync(object? sender, ShutdownEventArgs args)
        {
            FailAll("Канал публикации закрыт до подтверждения сообщений.");
            return Task.CompletedTask;
        }

        private void CompleteUpTo(ulong deliveryTag, bool confirmed)
        {
            KeyValuePair<ulong, TaskCompletionSource>[] matched;
            lock (_gate)
            {
                if (_pending.Count == 0)
                    return;

                matched = _pending.Where(kv => kv.Key <= deliveryTag).ToArray();
                foreach (var kv in matched)
                    _pending.Remove(kv.Key);
            }

            foreach (var kv in matched)
            {
                if (confirmed)
                    kv.Value.TrySetResult();
                else
                    kv.Value.TrySetException(new InvalidOperationException("RabbitMQ отклонил публикацию (nack)."));
            }
        }

        private void FailAll(string reason)
        {
            TaskCompletionSource[] all;
            lock (_gate)
            {
                all = _pending.Values.ToArray();
                _pending.Clear();
                _failed = true;
            }

            foreach (var tcs in all)
                tcs.TrySetException(new InvalidOperationException(reason));
        }

        public void Dispose()
        {
            _channel.BasicAcksAsync -= OnAckAsync;
            _channel.BasicNacksAsync -= OnNackAsync;
            _channel.ChannelShutdownAsync -= OnShutdownAsync;
            FailAll("Транспорт остановлен.");
        }
    }
}
