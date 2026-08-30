using System.Collections.Concurrent;
using AvtoBus.Observability;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AvtoBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus-транспорт (идеи 61–62): <see cref="ITransport"/> поверх Azure.Messaging.ServiceBus.
///
/// Семантика:
/// — Очередь и топик: очередь — ServiceBus Queue (один владелец), топик — ServiceBus Topic с
///   подпиской на группу консьюмеров (каждая группа — своя подписка, получает копию).
/// — Подтверждение = CompleteMessage. Reject(requeue) = AbandonMessage (брокер вернёт сообщение
///   с инкрементом DeliveryCount); Reject(без requeue) = DeadLetterMessage.
/// — Сессии (идея 61): PartitionKey → SessionId — строгий порядок внутри сессии.
/// — Отложенные (идея 86-совместимо): ScheduledEnqueueTime — натив.
/// — Lock renew (идея 62): фоновая задача продлевает lock, пока хендлер работает.
/// </summary>
public sealed class AsbTransport : ITransport, IConsumerLagProvider, IDisposable
{
    private readonly AsbOptions _options;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ConcurrentDictionary<string, long> _consumerLags = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _provisioned = new(StringComparer.Ordinal);
    private int _disposed;

    public AsbTransport(AsbOptions options)
    {
        _options = options;

        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets,
        };

        _client = new ServiceBusClient(options.ConnectionString, clientOptions);
        _admin = new ServiceBusAdministrationClient(options.ConnectionString);
    }

    public string Name => "asb";

    /// <summary>Оценка лага из ActiveMessageCount очереди/подписки — для метрики consumer.lag.</summary>
    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        await EnsureProvisionedAsync(destination, ct).ConfigureAwait(false);

        var sender = _senders.GetOrAdd(destination.Name, n => _client.CreateSender(n));

        var message = AsbEnvelopeSerializer.ToMessage(envelope);
        message.SessionId = _options.RequireSessions ? envelope.PartitionKey : null;

        if (envelope.DeliverAt is { } deliverAt)
        {
            var sequence = await sender.ScheduleMessageAsync(message, deliverAt.UtcDateTime, ct).ConfigureAwait(false);
            _ = sequence;
        }
        else
        {
            await sender.SendMessageAsync(message, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Один консьюмер на подписку; очередь — приёмник на саму очередь, топик — приёмник на
    /// подписку группы. Сообщения сессий упорядочены (идея 61); lock продлевается фоном (идея 62).
    /// </summary>
    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var destination = subscription.Destination;
        var group = subscription.ConsumerGroup;
        var path = destination.Kind == DestinationKind.Topic
            ? await SubscriptionPathAsync(destination.Name, group, ct).ConfigureAwait(false)
            : destination.Name;

        var receiverOptions = new ServiceBusReceiverOptions
        {
            PrefetchCount = _options.PrefetchCount,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        };

        await using var receiver = _options.RequireSessions
            ? await _client.AcceptNextSessionAsync(path, new ServiceBusSessionReceiverOptions
            {
                PrefetchCount = _options.PrefetchCount,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            }, ct).ConfigureAwait(false)
            : _client.CreateReceiver(path, receiverOptions);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
                if (received is null)
                    continue;

                TrackLag(path);

                Envelope envelope;
                try
                {
                    envelope = AsbEnvelopeSerializer.FromMessage(received);
                }
                catch (InvalidDataException)
                {
                    // Несовместимый продюсер — dead-letter, чтобы не зациклиться на мусоре.
                    await receiver.DeadLetterMessageAsync(received, new Dictionary<string, object?>
                    {
                        ["reason"] = "invalid-envelope",
                    }, ct).ConfigureAwait(false);
                    continue;
                }

                var message = new AsbMessage(receiver, received, envelope, path);
                yield return message;
            }
        }
        finally
        {
            // В ASB нет «отписаться» — сообщения остаются в подписке; приёмник закрывается.
        }
    }

    /// <summary>Путь к подписке группы: топик + подписка создаются идемпотентно.</summary>
    private async ValueTask<string> SubscriptionPathAsync(string topic, string group, CancellationToken ct)
    {
        var subscription = $"{topic}/{group}";
        try
        {
            await _admin.CreateSubscriptionAsync(
                new CreateSubscriptionOptions(topic, group)
                {
                    DefaultMessageTimeToLive = _options.DefaultMessageTimeToLive ?? TimeSpan.FromDays(14),
                },
                ct).ConfigureAwait(false);
        }
        catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Подписка уже существует — штатно.
        }

        return subscription;
    }

    private long _lastLagCheckTicks;
    private void TrackLag(string path)
    {
        var now = DateTimeOffset.UtcNow;
        if (Interlocked.Read(ref _lastLagCheckTicks) != 0)
        {
            var last = new DateTimeOffset(Interlocked.Read(ref _lastLagCheckTicks), TimeSpan.Zero);
            if (now - last < TimeSpan.FromSeconds(30)) return;
        }
        Interlocked.Exchange(ref _lastLagCheckTicks, now.UtcTicks);
        _ = Task.Run(async () =>
        {
            try
            {
                if (path.Contains('/', StringComparison.Ordinal))
                {
                    var parts = path.Split('/', 2);
                    var props = await _admin.GetSubscriptionRuntimePropertiesAsync(parts[0], parts[1]).ConfigureAwait(false);
                    _consumerLags[path] = props.Value.ActiveMessageCount;
                }
                else
                {
                    var props = await _admin.GetQueueRuntimePropertiesAsync(path).ConfigureAwait(false);
                    _consumerLags[path] = props.Value.ActiveMessageCount;
                }
            }
            catch { }
        });
    }

    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        foreach (var destination in destinations)
            await EnsureProvisionedAsync(destination, ct).ConfigureAwait(false);
    }

    private async ValueTask EnsureProvisionedAsync(TransportDestination destination, CancellationToken ct)
    {
        var key = $"{destination.Kind}:{destination.Name}";
        if (_provisioned.ContainsKey(key))
            return;

        if (destination.Kind == DestinationKind.Topic)
            await EnsureTopicAsync(destination.Name, ct).ConfigureAwait(false);
        else
            await EnsureQueueAsync(destination.Name, ct).ConfigureAwait(false);

        _provisioned[key] = 1;
    }

    private async ValueTask EnsureQueueAsync(string name, CancellationToken ct)
    {
        try
        {
            await _admin.CreateQueueAsync(new CreateQueueOptions(name)
            {
                RequiresSession = _options.RequireSessions,
                DefaultMessageTimeToLive = _options.DefaultMessageTimeToLive ?? TimeSpan.FromDays(14),
                MaxDeliveryCount = 10,
            }, ct).ConfigureAwait(false);
        }
        catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Штатно.
        }
    }

    private async ValueTask EnsureTopicAsync(string name, CancellationToken ct)
    {
        try
        {
            await _admin.CreateTopicAsync(new CreateTopicOptions(name)
            {
                RequiresDuplicateDetection = true,
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(2),
            }, ct).ConfigureAwait(false);
        }
        catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Штатно.
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        foreach (var s in _senders.Values)
            s.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>ITransport : IAsyncDisposable — синхронного Dispose достаточно.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Сообщение из ASB. Ack = CompleteMessage; Reject(requeue) = AbandonMessage (брокер
    /// передоставит с инкрементом DeliveryCount); Reject(без requeue) = DeadLetterMessage.
    /// </summary>
    private sealed class AsbMessage : ITransportMessage
    {
        private readonly ServiceBusReceiver _receiver;
        private readonly ServiceBusReceivedMessage _received;
        private readonly string _path;
        private readonly CancellationTokenSource _renewCts;
        private readonly Task _renewTask;
        private int _settled;

        public AsbMessage(ServiceBusReceiver receiver, ServiceBusReceivedMessage received, Envelope envelope, string path)
        {
            _receiver = receiver;
            _received = received;
            _path = path;
            Envelope = envelope;

            _renewCts = new CancellationTokenSource();
            _renewTask = RenewLockAsync(_renewCts.Token);
        }

        public Envelope Envelope { get; }

        public TransportDestination Source => TransportDestination.Queue(_path);

        private async Task RenewLockAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
                    await _receiver.RenewMessageLockAsync(_received, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
                // Lock мог истечь или связь упала — дальше не продлеваем.
            }
        }

        public async ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            try { await _renewCts.CancelAsync().ConfigureAwait(false); } catch { }
            _renewCts.Dispose();
            await _receiver.CompleteMessageAsync(_received, ct).ConfigureAwait(false);
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            try { await _renewCts.CancelAsync().ConfigureAwait(false); } catch { }
            _renewCts.Dispose();

            if (requeue)
            {
                // Requeue: возврат брокеру — пере-доставка с инкрементом DeliveryCount.
                await _receiver.AbandonMessageAsync(_received, cancellationToken: ct).ConfigureAwait(false);
            }
            else
            {
                // Без requeue: dead-letter — дальше решает ядро (DLQ).
                await _receiver.DeadLetterMessageAsync(_received, new Dictionary<string, object?>
                {
                    ["reason"] = "rejected",
                }, ct).ConfigureAwait(false);
            }
        }
    }
}
