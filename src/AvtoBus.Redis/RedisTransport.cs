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

    // Sync ctor kept for backward compat; async factory available via CreateAsync
    public RedisTransport(RedisOptions options)
    {
        _options = options;
        _redis = ConnectionMultiplexer.Connect(options.Configuration);
        _db = _redis.GetDatabase();
    }

    public static async Task<RedisTransport> CreateAsync(RedisOptions options, CancellationToken ct = default)
    {
        var redis = await ConnectionMultiplexer.ConnectAsync(options.Configuration).ConfigureAwait(false);
        return new RedisTransport(redis, options);
    }

    private RedisTransport(ConnectionMultiplexer redis, RedisOptions options)
    {
        _options = options;
        _redis = redis;
        _db = _redis.GetDatabase();
    }

    public string Name => TransportNames.Redis;

    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        var stream = StreamKey(destination);
        var entries = RedisEnvelopeSerializer.ToEntries(envelope);

        if (_options.MaxStreamLength > 0)
            await _db.StreamAddAsync(stream, entries, maxLength: _options.MaxStreamLength, useApproximateMaxLength: true).ConfigureAwait(false);
        else
            await _db.StreamAddAsync(stream, entries).ConfigureAwait(false);
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
            catch (RedisTimeoutException)
            {
                await Task.Delay(200, ct).ConfigureAwait(false);
                continue;
            }
            catch (RedisException ex) when (ex.Message.Contains("NOGROUP", StringComparison.Ordinal))
            {
                try { await _db.StreamCreateConsumerGroupAsync(stream, group, position: 0, createStream: true).ConfigureAwait(false); }
                catch { }
                await Task.Delay(200, ct).ConfigureAwait(false);
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

    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastReclaimAt = new(StringComparer.Ordinal);

    private async Task<IReadOnlyList<StreamEntry>> ReclaimAsync(
        string stream, string group, string consumerName, CancellationToken ct)
    {
        var reclaimKey = $"{stream}:{group}";
        var now = DateTimeOffset.UtcNow;
        if (_lastReclaimAt.TryGetValue(reclaimKey, out var last) && now - last < TimeSpan.FromMilliseconds(_options.MinIdleTimeMs / 2))
            return Array.Empty<StreamEntry>();
        _lastReclaimAt[reclaimKey] = now;

        try
        {
            var cursorKey = $"{stream}:{group}";
            var startAtId = _claimCursors.GetOrAdd(cursorKey, "0-0");
            // Single roundtrip: StreamAutoClaim returns entries directly
            var result = await _db.StreamAutoClaimAsync(
                stream,
                group,
                consumerName,
                minIdleTimeInMs: _options.MinIdleTimeMs,
                startAtId: startAtId,
                count: _options.BatchSize).ConfigureAwait(false);

            if (result.ClaimedEntries.Length == 0)
            {
                // No more pending at this cursor — reset to beginning for next cycle
                _claimCursors[cursorKey] = "0-0";
                return Array.Empty<StreamEntry>();
            }

            var nextId = result.NextStartId;
            _claimCursors[cursorKey] = nextId.HasValue ? nextId.ToString() : result.ClaimedEntries[^1].Id.ToString();
            return result.ClaimedEntries;
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
            TrackLagAsync(destination.Name);
            return new RedisMessage(_db, entry, envelope, destination, group);
        }
        catch (InvalidDataException)
        {
            _ = _db.StreamAcknowledgeAsync(StreamKey(destination), group, entry.Id);
            return null;
        }
    }

    private long _lastLagCheckTicks;
    private void TrackLagAsync(string stream)
    {
        var now = DateTimeOffset.UtcNow;
        if (Interlocked.Read(ref _lastLagCheckTicks) != 0)
        {
            var last = new DateTimeOffset(Interlocked.Read(ref _lastLagCheckTicks), TimeSpan.Zero);
            if (now - last < TimeSpan.FromSeconds(5)) return;
        }
        Interlocked.Exchange(ref _lastLagCheckTicks, now.UtcTicks);
        _ = Task.Run(async () =>
        {
            try { _consumerLags[stream] = await _db.StreamLengthAsync(stream).ConfigureAwait(false); }
            catch (RedisException) { }
        });
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
