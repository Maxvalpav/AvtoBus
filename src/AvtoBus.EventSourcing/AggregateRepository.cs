namespace AvtoBus.EventSourcing;

/// <summary>Репозиторий агрегатов: загрузка из истории (+снапшот), сохранение с публикацией.</summary>
public interface IAggregateRepository
{
    ValueTask<TAggregate?> LoadAsync<TAggregate>(Guid id, CancellationToken ct = default)
        where TAggregate : Aggregate, new();

    ValueTask<TAggregate> LoadOrCreateAsync<TAggregate>(Guid id, CancellationToken ct = default)
        where TAggregate : Aggregate, new();

    /// <summary>Загрузить состояние на момент времени (time-travel).</summary>
    ValueTask<TAggregate?> LoadAsOfAsync<TAggregate>(Guid id, DateTimeOffset asOf, CancellationToken ct = default)
        where TAggregate : Aggregate, new();

    ValueTask<AppendResult> SaveAsync<TAggregate>(TAggregate aggregate, CancellationToken ct = default)
        where TAggregate : Aggregate;
}

public sealed class AggregateRepository : IAggregateRepository
{
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly SnapshotPolicy _snapshotPolicy;
    private readonly IBus? _bus;
    private readonly TimeProvider _clock;

    public AggregateRepository(
        IEventStore store,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        SnapshotPolicy snapshotPolicy,
        TimeProvider clock,
        IBus? bus = null)
    {
        _store = store;
        _serializer = serializer;
        _upcasters = upcasters;
        _snapshotPolicy = snapshotPolicy;
        _clock = clock;
        _bus = bus;
    }

    public async ValueTask<TAggregate?> LoadAsync<TAggregate>(Guid id, CancellationToken ct = default)
        where TAggregate : Aggregate, new()
    {
        var aggregate = new TAggregate { Id = id };
        var fromVersion = 0;

        var snapshot = await _store.LoadSnapshotAsync(id, ct);
        if (snapshot is not null && snapshot.StateType == typeof(TAggregate).FullName)
        {
            aggregate = _serializer.DeserializeSnapshot<TAggregate>(snapshot.Data);
            aggregate.Id = id;
            aggregate.Version = snapshot.Version;
            fromVersion = snapshot.Version;
        }

        var found = fromVersion > 0;
        await foreach (var stored in _store.ReadStreamAsync(id, fromVersion, ct: ct))
        {
            found = true;
            aggregate.Replay(Upcast(stored));
        }

        return found ? aggregate : null;
    }

    public async ValueTask<TAggregate> LoadOrCreateAsync<TAggregate>(Guid id, CancellationToken ct = default)
        where TAggregate : Aggregate, new()
        => await LoadAsync<TAggregate>(id, ct) ?? new TAggregate { Id = id };

    public async ValueTask<TAggregate?> LoadAsOfAsync<TAggregate>(
        Guid id, DateTimeOffset asOf, CancellationToken ct = default)
        where TAggregate : Aggregate, new()
    {
        var aggregate = new TAggregate { Id = id };
        var found = false;

        await foreach (var stored in _store.ReadStreamAsync(id, ct: ct))
        {
            if (stored.Timestamp > asOf) break;
            found = true;
            aggregate.Replay(Upcast(stored));
        }

        return found ? aggregate : null;
    }

    public async ValueTask<AppendResult> SaveAsync<TAggregate>(
        TAggregate aggregate, CancellationToken ct = default)
        where TAggregate : Aggregate
    {
        var events = aggregate.UncommittedEvents;
        if (events.Count == 0)
            return new AppendResult(aggregate.Version, 0, 0);

        var streamType = MessageTypeNaming.ToKebabCase(typeof(TAggregate).Name);
        var result = await _store.AppendAsync(
            aggregate.Id, streamType, events, aggregate.Version, ct);

        if (_bus is not null)
        {
            foreach (var e in events)
                await _bus.PublishAsync(e.Payload, new PublishOptions
                {
                    PartitionKey = aggregate.Id.ToString(),
                }, ct);
        }

        aggregate.Version = result.NewVersion;
        aggregate.MarkCommitted();

        if (_snapshotPolicy.ShouldSnapshot(typeof(TAggregate), result.NewVersion, events.Count))
        {
            await _store.SaveSnapshotAsync(new StoredSnapshot
            {
                StreamId = aggregate.Id,
                Version = result.NewVersion,
                StateType = typeof(TAggregate).FullName!,
                Data = _serializer.SerializeSnapshot(aggregate),
                CreatedAt = _clock.GetUtcNow(),
            }, ct);
        }

        return result;
    }

    private object Upcast(StoredEvent stored)
    {
        var @event = _serializer.Deserialize(stored.Data, stored.EventType);
        return _upcasters.Upcast(@event, stored.EventType, stored.SchemaVersion);
    }
}

/// <summary>Политика создания снапшотов.</summary>
public sealed class SnapshotPolicy
{
    private readonly Dictionary<Type, int> _everyNEvents = new();

    public int DefaultEveryNEvents { get; set; } = 100;

    public void For<TAggregate>(int everyNEvents) where TAggregate : Aggregate
        => _everyNEvents[typeof(TAggregate)] = everyNEvents;

    public bool ShouldSnapshot(Type aggregateType, int newVersion, int appendedCount)
    {
        var threshold = _everyNEvents.GetValueOrDefault(aggregateType, DefaultEveryNEvents);
        if (threshold <= 0) return false;
        var previousVersion = newVersion - appendedCount;
        return newVersion / threshold > previousVersion / threshold;
    }
}
