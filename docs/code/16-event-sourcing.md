# AvtoBus.EventSourcing — Event Store, агрегаты, проекции

---

## AvtoBus.EventSourcing/IEventStore.cs

```csharp
namespace AvtoBus.EventSourcing;

public interface IEventStore
{
    ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        int expectedVersion,
        IReadOnlyList<object> events,
        EventMetadata? metadata = null,
        CancellationToken ct = default);

    ValueTask<StreamSlice> ReadStreamAsync(
        Guid streamId,
        int fromVersion = 0,
        int maxCount = int.MaxValue,
        CancellationToken ct = default);

    IAsyncEnumerable<PersistedEvent> ReadAllAsync(
        long fromGlobalSeq = 0,
        CancellationToken ct = default);

    ValueTask<Snapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default);

    ValueTask SaveSnapshotAsync(Snapshot snapshot, CancellationToken ct = default);
}

public sealed record AppendResult(int NewVersion, long GlobalSequence);

public sealed record StreamSlice(
    Guid StreamId,
    int LastVersion,
    IReadOnlyList<PersistedEvent> Events);

public sealed record PersistedEvent
{
    public required long GlobalSequence { get; init; }
    public required Guid StreamId { get; init; }
    public required int Version { get; init; }
    public required string EventType { get; init; }
    public required byte[] Data { get; init; }
    public byte[]? Metadata { get; init; }
    public DateTime Timestamp { get; init; }
}

public sealed class EventMetadata
{
    public Guid? CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
    public string? UserId { get; set; }
    public string? TenantId { get; set; }
    public string? Source { get; set; }
    public Dictionary<string, string>? Custom { get; set; }
}

public sealed class Snapshot
{
    public Guid StreamId { get; set; }
    public int Version { get; set; }
    public string SnapshotType { get; set; } = "";
    public byte[] Data { get; set; } = [];
    public DateTime CreatedAt { get; set; }
}

public sealed class ConcurrencyException(Guid streamId, int expected, int actual)
    : Exception($"Stream {streamId}: expected version {expected}, actual {actual}")
{
    public Guid StreamId { get; } = streamId;
    public int ExpectedVersion { get; } = expected;
    public int ActualVersion { get; } = actual;
}
```

---

## AvtoBus.EventSourcing/PostgresEventStore.cs

```csharp
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System.Runtime.CompilerServices;

namespace AvtoBus.EventSourcing;

public sealed class PostgresEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly ISerializer _serializer;
    private readonly ITypeResolver _typeResolver;

    public PostgresEventStore(string cs, ISerializer serializer, ITypeResolver types)
    {
        _connectionString = cs;
        _serializer = serializer;
        _typeResolver = types;
    }

    public async ValueTask<AppendResult> AppendAsync(
        Guid streamId, int expectedVersion, IReadOnlyList<object> events,
        EventMetadata? metadata = null, CancellationToken ct = default)
    {
        if (events.Count == 0)
            return new AppendResult(expectedVersion, 0);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Проверка версии
        var currentVersion = await GetVersionAsync(conn, streamId, ct);
        if (expectedVersion != -1 && currentVersion != expectedVersion)
            throw new ConcurrencyException(streamId, expectedVersion, currentVersion);

        long lastGlobalSeq = 0;
        var newVersion = currentVersion;

        foreach (var evt in events)
        {
            newVersion++;
            var eventType = _typeResolver.GetName(evt.GetType());
            var data = _serializer.Serialize(evt).ToArray();
            var metaBytes = metadata is null ? null : _serializer.Serialize(metadata).ToArray();

            await using var cmd = new NpgsqlCommand("""
                INSERT INTO avtobus_events(stream_id, version, event_type, data, metadata, timestamp)
                VALUES (@stream, @version, @type, @data, @meta, now())
                RETURNING global_sequence
                """, conn, tx);
            cmd.Parameters.AddWithValue("stream", streamId);
            cmd.Parameters.AddWithValue("version", newVersion);
            cmd.Parameters.AddWithValue("type", eventType);
            cmd.Parameters.AddWithValue("data", data);
            cmd.Parameters.AddWithValue("meta", (object?)metaBytes ?? DBNull.Value);

            lastGlobalSeq = (long)(await cmd.ExecuteScalarAsync(ct))!;
        }

        await tx.CommitAsync(ct);
        return new AppendResult(newVersion, lastGlobalSeq);
    }

    public async ValueTask<StreamSlice> ReadStreamAsync(
        Guid streamId, int fromVersion = 0, int maxCount = int.MaxValue,
        CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand("""
            SELECT global_sequence, version, event_type, data, metadata, timestamp
            FROM avtobus_events
            WHERE stream_id = @stream AND version > @from
            ORDER BY version
            LIMIT @limit
            """, conn);
        cmd.Parameters.AddWithValue("stream", streamId);
        cmd.Parameters.AddWithValue("from", fromVersion);
        cmd.Parameters.AddWithValue("limit", maxCount);

        var events = new List<PersistedEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            events.Add(new PersistedEvent
            {
                GlobalSequence = reader.GetInt64(0),
                StreamId = streamId,
                Version = reader.GetInt32(1),
                EventType = reader.GetString(2),
                Data = (byte[])reader.GetValue(3),
                Metadata = reader.IsDBNull(4) ? null : (byte[])reader.GetValue(4),
                Timestamp = reader.GetDateTime(5),
            });
        }

        var lastVersion = events.Count > 0 ? events[^1].Version : fromVersion;
        return new StreamSlice(streamId, lastVersion, events);
    }

    public async IAsyncEnumerable<PersistedEvent> ReadAllAsync(
        long fromGlobalSeq = 0,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand("""
            SELECT global_sequence, stream_id, version, event_type, data, metadata, timestamp
            FROM avtobus_events
            WHERE global_sequence > @from
            ORDER BY global_sequence
            """, conn);
        cmd.Parameters.AddWithValue("from", fromGlobalSeq);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            yield return new PersistedEvent
            {
                GlobalSequence = reader.GetInt64(0),
                StreamId = reader.GetGuid(1),
                Version = reader.GetInt32(2),
                EventType = reader.GetString(3),
                Data = (byte[])reader.GetValue(4),
                Metadata = reader.IsDBNull(5) ? null : (byte[])reader.GetValue(5),
                Timestamp = reader.GetDateTime(6),
            };
        }
    }

    public async ValueTask<Snapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT stream_id, version, snapshot_type, data, created_at
            FROM avtobus_snapshots
            WHERE stream_id = @stream
            ORDER BY version DESC LIMIT 1
            """, conn);
        cmd.Parameters.AddWithValue("stream", streamId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;

        return new Snapshot
        {
            StreamId = reader.GetGuid(0),
            Version = reader.GetInt32(1),
            SnapshotType = reader.GetString(2),
            Data = (byte[])reader.GetValue(3),
            CreatedAt = reader.GetDateTime(4),
        };
    }

    public async ValueTask SaveSnapshotAsync(Snapshot snapshot, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_snapshots(stream_id, version, snapshot_type, data, created_at)
            VALUES (@stream, @version, @type, @data, now())
            ON CONFLICT (stream_id, version) DO NOTHING
            """, conn);
        cmd.Parameters.AddWithValue("stream", snapshot.StreamId);
        cmd.Parameters.AddWithValue("version", snapshot.Version);
        cmd.Parameters.AddWithValue("type", snapshot.SnapshotType);
        cmd.Parameters.AddWithValue("data", snapshot.Data);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> GetVersionAsync(NpgsqlConnection conn, Guid streamId, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT COALESCE(MAX(version), 0) FROM avtobus_events WHERE stream_id = @s", conn);
        cmd.Parameters.AddWithValue("s", streamId);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct) ?? 0);
    }
}
```

---

## AvtoBus.EventSourcing/Aggregate.cs

```csharp
namespace AvtoBus.EventSourcing;

/// <summary>
/// Базовый агрегат: собирает события, применяет к состоянию.
/// Стиль Axon: [EventHandler] методы применяются автоматически.
/// </summary>
public abstract class Aggregate
{
    private readonly List<object> _pendingEvents = new();
    public Guid Id { get; protected set; }
    public int Version { get; internal set; }

    public IReadOnlyList<object> PendingEvents => _pendingEvents;

    protected void Apply(object @event)
    {
        _pendingEvents.Add(@event);
        Mutate(@event);
    }

    /// <summary>
    /// Применить событие к состоянию (Evolve).
    /// Пользователь переопределяет через switch или dispatch table.
    /// </summary>
    protected abstract void Mutate(object @event);

    internal void MarkCommitted(int newVersion)
    {
        _pendingEvents.Clear();
        Version = newVersion;
    }

    internal void RestoreFrom(IEnumerable<object> history)
    {
        foreach (var evt in history)
            Mutate(evt);
    }
}
```

Пример:

```csharp
public sealed class OrderAggregate : Aggregate
{
    public string CustomerId { get; private set; } = "";
    public decimal Total { get; private set; }
    public OrderStatus Status { get; private set; }

    public static OrderAggregate Place(Guid id, string customerId, decimal total)
    {
        var agg = new OrderAggregate();
        agg.Apply(new OrderPlaced(id, customerId, total));
        return agg;
    }

    public void MarkPaid() => Apply(new OrderPaid(Id));
    public void Cancel(string reason) => Apply(new OrderCancelled(Id, reason));

    protected override void Mutate(object @event)
    {
        switch (@event)
        {
            case OrderPlaced e:    Id = e.OrderId; CustomerId = e.CustomerId; Total = e.Total; Status = OrderStatus.Placed; break;
            case OrderPaid _:      Status = OrderStatus.Paid; break;
            case OrderCancelled _: Status = OrderStatus.Cancelled; break;
        }
    }
}
```

---

## AvtoBus.EventSourcing/Repository.cs

```csharp
namespace AvtoBus.EventSourcing;

public interface IAggregateRepository<TAggregate> where TAggregate : Aggregate, new()
{
    ValueTask<TAggregate?> LoadAsync(Guid id, CancellationToken ct = default);
    ValueTask SaveAsync(TAggregate aggregate, CancellationToken ct = default);
}

public sealed class AggregateRepository<TAggregate> : IAggregateRepository<TAggregate>
    where TAggregate : Aggregate, new()
{
    private readonly IEventStore _store;
    private readonly ISerializer _serializer;
    private readonly ITypeResolver _types;

    public AggregateRepository(IEventStore store, ISerializer serializer, ITypeResolver types)
    {
        _store = store;
        _serializer = serializer;
        _types = types;
    }

    public async ValueTask<TAggregate?> LoadAsync(Guid id, CancellationToken ct = default)
    {
        var aggregate = new TAggregate();
        var startVersion = 0;

        // Load snapshot if available
        var snapshot = await _store.LoadSnapshotAsync(id, ct);
        if (snapshot is not null)
        {
            var snapType = _types.GetType(snapshot.SnapshotType);
            if (snapType is not null)
            {
                var state = _serializer.Deserialize(snapshot.Data, snapType);
                // Пользовательский метод RestoreFromSnapshot можно вызвать через рефлексию
                aggregate.GetType().GetMethod("RestoreFromSnapshot")?.Invoke(aggregate, new[] { state });
                startVersion = snapshot.Version;
                aggregate.Version = snapshot.Version;
            }
        }

        // Replay events after snapshot
        var slice = await _store.ReadStreamAsync(id, fromVersion: startVersion, ct: ct);
        if (slice.Events.Count == 0 && startVersion == 0)
            return null;

        var events = slice.Events.Select(e =>
        {
            var type = _types.GetType(e.EventType)!;
            return _serializer.Deserialize(e.Data, type);
        });

        aggregate.RestoreFrom(events);
        aggregate.Version = slice.LastVersion;
        return aggregate;
    }

    public async ValueTask SaveAsync(TAggregate aggregate, CancellationToken ct = default)
    {
        if (aggregate.PendingEvents.Count == 0) return;

        var result = await _store.AppendAsync(
            aggregate.Id,
            aggregate.Version,
            aggregate.PendingEvents,
            ct: ct);

        aggregate.MarkCommitted(result.NewVersion);
    }
}
```

---

## AvtoBus.EventSourcing/Projection.cs

```csharp
namespace AvtoBus.EventSourcing;

public abstract class Projection<TView>
{
    public abstract string Name { get; }

    /// <summary>
    /// Применить событие к текущей проекции.
    /// </summary>
    public abstract ValueTask ApplyAsync(TView? current, PersistedEvent evt, CancellationToken ct);
}
```

---

## AvtoBus.EventSourcing/ProjectionDaemon.cs

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.EventSourcing;

/// <summary>
/// Фоновый демон, перечитывающий события и обновляющий проекции.
/// </summary>
public sealed class ProjectionDaemon : BackgroundService
{
    private readonly IEventStore _store;
    private readonly ICheckpointStore _checkpoints;
    private readonly IEnumerable<IProjectionRunner> _runners;
    private readonly ILogger<ProjectionDaemon> _log;

    public ProjectionDaemon(
        IEventStore store,
        ICheckpointStore checkpoints,
        IEnumerable<IProjectionRunner> runners,
        ILogger<ProjectionDaemon> log)
    {
        _store = store;
        _checkpoints = checkpoints;
        _runners = runners;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = _runners.Select(r => RunProjection(r, stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task RunProjection(IProjectionRunner runner, CancellationToken ct)
    {
        _log.LogInformation("Starting projection {Name}", runner.Name);
        var checkpoint = await _checkpoints.GetAsync(runner.Name, ct);

        while (!ct.IsCancellationRequested)
        {
            var count = 0;
            await foreach (var evt in _store.ReadAllAsync(fromGlobalSeq: checkpoint, ct))
            {
                try
                {
                    await runner.HandleAsync(evt, ct);
                    checkpoint = evt.GlobalSequence;
                    count++;

                    // Периодически сохраняем чекпоинт
                    if (count % 100 == 0)
                        await _checkpoints.SaveAsync(runner.Name, checkpoint, ct);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Projection {Name} failed on seq={Seq}", runner.Name, evt.GlobalSequence);
                    throw;
                }
            }

            if (count > 0)
                await _checkpoints.SaveAsync(runner.Name, checkpoint, ct);

            // Ждём новые события
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }
}

public interface IProjectionRunner
{
    string Name { get; }
    ValueTask HandleAsync(PersistedEvent evt, CancellationToken ct);
}

public interface ICheckpointStore
{
    ValueTask<long> GetAsync(string projectionName, CancellationToken ct);
    ValueTask SaveAsync(string projectionName, long globalSequence, CancellationToken ct);
}
```

---

## Migrations для Event Store

```sql
CREATE TABLE avtobus_events (
    global_sequence BIGSERIAL PRIMARY KEY,
    stream_id UUID NOT NULL,
    version INT NOT NULL,
    event_type TEXT NOT NULL,
    data BYTEA NOT NULL,
    metadata BYTEA,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (stream_id, version)
);
CREATE INDEX ix_events_stream ON avtobus_events(stream_id, version);
CREATE INDEX ix_events_type ON avtobus_events(event_type);

CREATE TABLE avtobus_snapshots (
    stream_id UUID NOT NULL,
    version INT NOT NULL,
    snapshot_type TEXT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

CREATE TABLE avtobus_projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    global_sequence BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
