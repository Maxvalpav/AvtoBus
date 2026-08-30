using System.Collections.Concurrent;
using AvtoBus.Observability;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace AvtoBus.Nats;

/// <summary>
/// NATS/JetStream-транспорт (идеи 63–64): durable push-consumers с queue groups.
///
/// Семантика:
/// — Каждый destination — JetStream стрим (subject = имя destination).
/// — Группа консьюмеров = push-consumer со своим deliver-subject и queue group:
///   подписчики одной группы делят сообщения, разные группы получают копии (как Kafka).
/// — Back-pressure — через MaxAckPending (JetStream не шлёт сверх лимита неподтверждённых).
/// — Reject(requeue) = NakAsync → JetStream пере-доставит с инкрементом NumDelivered.
/// — Reject(без requeue) = AckTerminate → сообщение выпадает из доставки.
/// </summary>
public sealed class NatsTransport : ITransport, IConsumerLagProvider, IDisposable
{
    private readonly NatsOptions _options;
    private readonly NatsConnection _connection;
    private readonly INatsJSContext _js;
    private readonly ConcurrentDictionary<string, long> _consumerLags = new(StringComparer.Ordinal);
    private int _disposed;

    public NatsTransport(NatsOptions options)
    {
        _options = options;
        var opts = NatsOpts.Default with { Url = options.Url };
        _connection = new NatsConnection(opts);
        _connection.ConnectAsync().GetAwaiter().GetResult();
        _js = new NatsJSContextFactory().CreateContext(_connection);
    }

    public static async Task<NatsTransport> CreateAsync(NatsOptions options, CancellationToken ct = default)
    {
        var opts = NatsOpts.Default with { Url = options.Url };
        var conn = new NatsConnection(opts);
        await conn.ConnectAsync().ConfigureAwait(false);
        var js = new NatsJSContextFactory().CreateContext(conn);
        return new NatsTransport(options, conn, js);
    }

    private NatsTransport(NatsOptions options, NatsConnection connection, INatsJSContext js)
    {
        _options = options;
        _connection = connection;
        _js = js;
    }

    public string Name => "nats";

    /// <summary>Оценка лага группы (из ConsumerInfo.NumPending) — для метрики consumer.lag.</summary>
    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        var subject = SubjectName(destination);
        var (headers, body) = NatsEnvelopeSerializer.ToNats(envelope);

        await _js.PublishAsync(
            subject,
            body,
            opts: new NatsJSPubOpts
            {
                MsgId = envelope.MessageId.ToString("N"),
            },
            headers: headers,
            cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Один push-consumer на группу; все подписчики группы слушают его deliver-subject
    /// в одной queue group и делят сообщения. Back-pressure через MaxAckPending (идея 63).
    /// </summary>
    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var destination = subscription.Destination;
        var subject = SubjectName(destination);
        var group = subscription.ConsumerGroup;

        var deliverSubject = $"{subject}.dlv.{group}";
        var durable = $"{group}";

        var consumerConfig = new ConsumerConfig
        {
            DurableName = durable,
            DeliverSubject = deliverSubject,
            DeliverGroup = group,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            MaxAckPending = subscription.PrefetchCount,
            MaxDeliver = _options.MaxDeliver,
            AckWait = _options.AckWait,
            FilterSubject = subject,
            Description = "AvtoBus",
        };

        try
        {
            await _js.CreateOrUpdateConsumerAsync(StreamName(destination), consumerConfig, ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Log topology error but don't hide auth failures forever
            await Task.Delay(1000, ct).ConfigureAwait(false);
            throw;
        }

        var subscriptionTask = _connection.SubscribeAsync<NatsJSMsg<byte[]>>(
            deliverSubject,
            queueGroup: group,
            opts: new NatsSubOpts
            {
                IdleTimeout = _options.FetchTimeout,
            },
            cancellationToken: ct);

        await using var enumerator = subscriptionTask.GetAsyncEnumerator(ct);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (NatsException)
                {
                    // Перерыв доставки (реконнект) — продолжаем опрос.
                    await Task.Delay(100, ct).ConfigureAwait(false);
                    continue;
                }

                if (!moved)
                {
                    if (ct.IsCancellationRequested)
                        yield break;

                    // Idle-таймаут подписки: сообщений не было. JetStream-консьюмер жив,
                    // подписка пересоздаётся следующим циклом.
                    await Task.Delay(50, ct).ConfigureAwait(false);
                    continue;
                }

                var jsMessage = enumerator.Current.Data;
                if (jsMessage.Data is null)
                    continue;

                TrackLag(subject, group, jsMessage);

                Envelope envelope;
                try
                {
                    envelope = NatsEnvelopeSerializer.FromNats(jsMessage);
                }
                catch (InvalidDataException)
                {
                    // Несовместимый продюсер — терминальный ack, чтобы не зациклиться на мусоре.
                    await jsMessage.AckTerminateAsync(cancellationToken: ct).ConfigureAwait(false);
                    continue;
                }

                var message = new NatsMessage(jsMessage, envelope, destination, group);
                yield return message;
            }
        }
        finally
        {
            // Consumer остаётся в JetStream (durable) — пере-подписка восстановит доставку.
        }
    }

    private void TrackLag(string subject, string group, NatsJSMsg<byte[]> message)
    {
        try
        {
            var pending = (long)(message.Metadata?.NumPending ?? 0);
            _consumerLags[$"{subject}:{group}"] = pending;
        }
        catch
        {
            // Метрика — наблюдательная; сбой не должен ломать обработку.
        }
    }

    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        foreach (var group in destinations.GroupBy(SubjectName))
        {
            var subject = group.Key;
            var streamName = StreamName(group.First());

            var config = new StreamConfig
            {
                Name = streamName,
                Subjects = [subject],
                Retention = StreamConfigRetention.Limits,
                Storage = _options.StorageType.Equals("memory", StringComparison.OrdinalIgnoreCase)
                    ? StreamConfigStorage.Memory
                    : StreamConfigStorage.File,
                MaxMsgs = _options.MaxMsgsPerStream,
                MaxAge = _options.MaxAge,
                NumReplicas = _options.Replicas,
                DuplicateWindow = TimeSpan.FromMinutes(2),
            };

            await _js.CreateOrUpdateStreamAsync(config, ct).ConfigureAwait(false);
        }
    }

    private static string SubjectName(TransportDestination destination) => destination.Name;

    /// <summary>Имя JetStream стрима: точки/дефисы недопустимы в имени, "_" экранируем чтобы избежать коллизий.</summary>
    private static string StreamName(TransportDestination destination)
        => "AVB_" + destination.Name.Replace("_", "__").Replace(".", "_dot_").Replace("-", "_dash_");

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>ITransport : IAsyncDisposable — синхронного Dispose достаточно.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Сообщение из JetStream. Ack = подтверждение; Reject(requeue) = Nak (пере-доставка,
    /// NumDelivered инкрементируется); Reject(без requeue) = AckTerminate (вне доставки).
    /// </summary>
    private sealed class NatsMessage : ITransportMessage
    {
        private readonly NatsJSMsg<byte[]> _message;
        private readonly string _stream;
        private int _settled;

        public NatsMessage(NatsJSMsg<byte[]> message, Envelope envelope, TransportDestination destination, string group)
        {
            _message = message;
            Envelope = envelope;
            _stream = destination.Name;
            Source = TransportDestination.Queue($"{destination.Name}:{group}");
        }

        public Envelope Envelope { get; }

        public TransportDestination Source { get; }

        public async ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            await _message.AckAsync(cancellationToken: ct).ConfigureAwait(false);
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue)
                await _message.NakAsync(cancellationToken: ct).ConfigureAwait(false);
            else
                await _message.AckTerminateAsync(cancellationToken: ct).ConfigureAwait(false);
        }
    }
}
