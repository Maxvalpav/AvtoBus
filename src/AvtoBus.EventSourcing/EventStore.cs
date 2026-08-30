using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace AvtoBus.EventSourcing;

/// <summary>
/// In-memory реализация <see cref="IEventStore"/> — эталон поведения и быстрые тесты без БД.
/// Задаёт семантику optimistic concurrency и головы потока. Не переживает рестарт.
/// </summary>
public sealed class InMemoryEventStore : IEventStore
{
    private readonly object _gate = new();
    private readonly IEventSerializer _serializer;
    private readonly TimeProvider _clock;
    private long _head;

    private readonly Dictionary<Guid, List<StoredEvent>> _streams = new();
    private readonly Dictionary<Guid, StreamMetadata> _meta = new();
    private readonly Dictionary<Guid, StoredSnapshot> _snapshots = new();
    private readonly List<StoredEvent> _all = new();

    public InMemoryEventStore(IEventSerializer serializer, TimeProvider? clock = null)
    {
        _serializer = serializer;
        _clock = clock ?? TimeProvider.System;
    }

    public ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        string streamType,
        IReadOnlyList<EventToAppend> events,
        int expectedVersion = -1,
        CancellationToken ct = default)
    {
        if (events.Count == 0)
            throw new ArgumentException("No events to append", nameof(events));

        lock (_gate)
        {
            var exists = _streams.TryGetValue(streamId, out var list);
            var actual = list?.Count ?? 0;

            if (expectedVersion >= 0 && actual != expectedVersion)
                throw new ConcurrencyException(streamId, expectedVersion, actual);
            if (expectedVersion == 0 && exists)
                throw new ConcurrencyException(streamId, 0, actual);

            if (list is null)
            {
                list = [];
                _streams[streamId] = list;
            }
            var now = _clock.GetUtcNow();
            var sequences = new List<long>(events.Count);

            foreach (var e in events)
            {
                var seq = ++_head;
                sequences.Add(seq);

                var stored = new StoredEvent
                {
                    GlobalSequence = seq,
                    StreamId = streamId,
                    StreamType = streamType,
                    Version = ++actual,
                    EventType = e.EventType,
                    SchemaVersion = e.SchemaVersion,
                    Data = _serializer.Serialize(e.Payload),
                    Metadata = EncodeMetadata(e.Metadata),
                    Timestamp = now,
                    CorrelationId = ParseGuid(e.Metadata.GetValueOrDefault("correlationId")),
                    CausationId = ParseGuid(e.Metadata.GetValueOrDefault("causationId")),
                    TenantId = e.Metadata.GetValueOrDefault("tenantId"),
                };

                list.Add(stored);
                _all.Add(stored);
            }

            var createdAt = _meta.TryGetValue(streamId, out var prev) ? prev.CreatedAt : now;
            _meta[streamId] = new StreamMetadata(
                streamId, streamType, actual, createdAt, now, prev?.IsArchived ?? false);

            return ValueTask.FromResult(new AppendResult(actual, sequences[0], sequences[^1]));
        }
    }

    public async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        Guid streamId,
        int fromVersion = 0,
        int? toVersion = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield();
        List<StoredEvent>? list;
        lock (_gate)
            list = _streams.TryGetValue(streamId, out var s)
                ? s.Where(e => e.Version > fromVersion && (toVersion is null || e.Version <= toVersion)).ToList()
                : null;

        if (list is null)
            yield break;

        foreach (var e in list)
        {
            ct.ThrowIfCancellationRequested();
            yield return e;
        }
    }

    public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
        long fromSequence = 0,
        int batchSize = 1000,
        IReadOnlyList<string>? eventTypeFilter = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield();
        List<StoredEvent> batch;
        lock (_gate)
            batch = _all
                .Where(e => e.GlobalSequence > fromSequence)
                .Where(e => eventTypeFilter is null || eventTypeFilter.Count == 0 || eventTypeFilter.Contains(e.EventType))
                .Take(batchSize)
                .ToList();

        foreach (var e in batch)
        {
            ct.ThrowIfCancellationRequested();
            yield return e;
        }
    }

    public async IAsyncEnumerable<StoredEvent> ReadCategoryAsync(
        string streamType,
        long fromSequence = 0,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield();
        List<StoredEvent> batch;
        lock (_gate)
            batch = _all
                .Where(e => e.StreamType == streamType && e.GlobalSequence > fromSequence)
                .ToList();

        foreach (var e in batch)
        {
            ct.ThrowIfCancellationRequested();
            yield return e;
        }
    }

    public ValueTask<StreamMetadata?> GetStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        lock (_gate)
            return ValueTask.FromResult(_meta.TryGetValue(streamId, out var m) ? m : null);
    }

    public ValueTask<long> GetHeadSequenceAsync(CancellationToken ct = default)
    {
        lock (_gate)
            return ValueTask.FromResult(_head);
    }

    public ValueTask SaveSnapshotAsync(StoredSnapshot snapshot, CancellationToken ct = default)
    {
        lock (_gate)
            _snapshots[snapshot.StreamId] = snapshot;
        return ValueTask.CompletedTask;
    }

    public ValueTask<StoredSnapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default)
    {
        lock (_gate)
            return ValueTask.FromResult(
                _snapshots.TryGetValue(streamId, out var s) ? s with { Data = s.Data.ToArray() } : null);
    }

    public ValueTask ArchiveStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        lock (_gate)
        {
            if (_meta.TryGetValue(streamId, out var m))
                _meta[streamId] = m with { IsArchived = true };
        }
        return ValueTask.CompletedTask;
    }

    private static ReadOnlyMemory<byte> EncodeMetadata(Dictionary<string, string> metadata)
    {
        if (metadata.Count == 0)
            return ReadOnlyMemory<byte>.Empty;
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(metadata);
    }

    private static Guid? ParseGuid(string? s) => Guid.TryParse(s, out var g) ? g : null;
}
