using System.Collections.Concurrent;
using AvtoBus.Observability;
using StackExchange.Redis;

namespace AvtoBus.Redis;

/// <summary>
/// Redis Streams-транспорт (идея 65): consumer groups + XAUTOCLAIM.
///
/// Семантика:
/// — Очередь и топик — оба Redis Streams; разница в группах: очередь читает один сервис
///   (несколько воркеров делят через одну consumer group), топик — каждая группа получает
///   свою копию стрима.
/// — Подтверждение = XACK. Reject(requeue) = пере-публикация с инкрементированным
///   DeliveryAttempt + XACK исходного; Reject(без requeue) = XACK (вне доставки; DLQ — на уровне ядра).
/// — XAUTOCLAIM: pending-сообщения, которые консьюмер не подтвердил дольше MinIdleTimeMs,
///   переподхватываются любым живым консьюмером группы (идея 65: переживание упавших воркеров).
/// </summary>
public sealed class RedisTransport : ITransport, IConsumerLagProvider, IDisposable
{
    private readonly RedisOptions _options;
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly ConcurrentDictionary<string, long> _consumerLags = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _claimCursors = new(StringComparer.Ordinal);
    private int _disposed;

    public RedisTransport(RedisOptions options)
    {
        _options = options;
        _redis = ConnectionMultiplexer.Connect(options.Configuration);
        _db = _redis.GetDatabase();
    }

    public string Name => "redis";

    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        var stream = StreamKey(destination);
        var entries = RedisEnvelopeSerializer.ToEntries(envelope);

        await _db.StreamAddAsync(
            stream,
            entries,
            maxLength: _options.MaxStreamLength,
            useApproximateMaxLength: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Один консьюмер на подписку; сообщения группы делятся нативной consumer group.
    /// XAUTOCLAIM-переподхват зависших выполняется перед каждым чтением (идея 65).
    /// </summary>
    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var destination = subscription.Destination;
        var stream = StreamKey(destination);
        var group = subscription.ConsumerGroup;
        var consumerName = $"{group}-{Guid.NewGuid():N}";

        // Группа создаётся один раз (идиемпотентно); позиция 0 — доставить всё с начала.
        try
        {
            await _db.StreamCreateConsumerGroupAsync(stream, group, position: 0, createStream: true).ConfigureAwait(false);
        }
        catch (RedisServerException exception) when (exception.Message.Contains("BUSYGROUP"))
        {
            // Группа уже существует — норм.
        }

        while (!ct.IsCancellationRequested)
        {
            // Переподхват зависших (XAUTOCLAIM): сообщения, не подтверждённые дольше
            // MinIdleTimeMs (например, консьюмер упал), переходят этому консьюмеру.
            foreach (var claimed in await ReclaimAsync(stream, group, consumerName, ct).ConfigureAwait(false))
            {
                var message = TryBuildMessage(claimed, destination, group);
                if (message is not null)
                    yield return message;
            }

            if (ct.IsCancellationRequested)
                yield break;

            StreamEntry[] batch;
            try
            {
                batch = await _db.StreamReadGroupAsync(
                    stream,
                    group,
                    consumerName,
                    position: ">",
                    count: _options.BatchSize).ConfigureAwait(false);
            }
            catch (RedisException)
            {
                await Task.Delay(100, ct).ConfigureAwait(false);
                continue;
            }

            if (batch.Length == 0)
            {
                await Task.Delay(100, ct).ConfigureAwait(false);
                continue;
            }

            foreach (var entry in batch)
            {
                var message = TryBuildMessage(entry, destination, group);
                if (message is not null)
                    yield return message;
            }
        }
    }

    private async Task<IReadOnlyList<StreamEntry>> ReclaimAsync(
        string stream, string group, string consumerName, CancellationToken ct)
    {
        try
        {
            var cursorKey = $"{stream}:{group}";
            var startAtId = _claimCursors.GetOrAdd(cursorKey, "0-0");
            var result = await _db.StreamAutoClaimIdsOnlyAsync(
                stream,
                group,
                consumerName,
                minIdleTimeInMs: _options.MinIdleTimeMs,
                startAtId: startAtId,
                count: _options.BatchSize).ConfigureAwait(false);

            if (result.ClaimedIds.Length == 0)
                return Array.Empty<StreamEntry>();

            // Продвигаем курсор, чтобы следующий скан продолжил с последнего ID (O(1) вместо O(N)).
            if (result.ClaimedIds.Length > 0)
                _claimCursors[cursorKey] = result.ClaimedIds[^1].ToString();

            return await _db.StreamClaimAsync(
                stream,
                group,
                consumerName,
                minIdleTimeInMs: _options.MinIdleTimeMs,
                messageIds: result.ClaimedIds).ConfigureAwait(false);
        }
        catch (RedisException)
        {
            return Array.Empty<StreamEntry>();
        }
    }

    private RedisMessage? TryBuildMessage(StreamEntry entry, TransportDestination destination, string group)
    {
        try
        {
            var envelope = RedisEnvelopeSerializer.FromEntry(entry);
            TrackLag(destination.Name, entry);
            return new RedisMessage(_db, entry, envelope, destination, group);
        }
        catch (InvalidDataException)
        {
            // Несовместимый продюсер — снимаем с доставки (ack в группе, из которой прочитали),
            // чтобы не зациклиться на мусоре.
            _db.StreamAcknowledge(destination.Name, group, entry.Id);
            return null;
        }
    }

    private void TrackLag(string stream, StreamEntry entry)
    {
        try
        {
            var length = _db.StreamLength(stream);
            // Приближение: длина стрима как «глубина» для консьюмеров без оффсет-метрик.
            _consumerLags[stream] = length;
        }
        catch (RedisException)
        {
            // Метрика — наблюдательная.
        }
    }

    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        // Стримы создаются лениво первой XADD; группа — первым XREADGROUP.
        // Провайдинг не требует явной топологии.
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static string StreamKey(TransportDestination destination) => destination.Name;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _redis.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Сообщение из Redis Stream. Ack = XACK; Reject(requeue) = XADD с инкрементом попытки
    /// + XACK исходного; Reject(без requeue) = XACK.
    /// </summary>
    private sealed class RedisMessage : ITransportMessage
    {
        private readonly IDatabase _db;
        private readonly string _stream;
        private readonly string _group;
        private readonly RedisValue _entryId;
        private int _settled;

        public RedisMessage(
            IDatabase db,
            StreamEntry entry,
            Envelope envelope,
            TransportDestination destination,
            string group)
        {
            _db = db;
            _entryId = entry.Id;
            _stream = destination.Name;
            _group = group;
            Envelope = envelope;
        }

        public Envelope Envelope { get; }

        public TransportDestination Source => TransportDestination.Queue(_stream);

        public async ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            await _db.StreamAcknowledgeAsync(_stream, _group, _entryId).ConfigureAwait(false);
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue)
            {
                // Requeue = пере-публикация с инкрементом попытки; исходное XACK-нуто.
                var entries = RedisEnvelopeSerializer.ToEntries(Envelope.NextAttempt());
                await _db.StreamAddAsync(_stream, entries, maxLength: 0, useApproximateMaxLength: false).ConfigureAwait(false);
            }

            await _db.StreamAcknowledgeAsync(_stream, _group, _entryId).ConfigureAwait(false);
        }
    }
}
