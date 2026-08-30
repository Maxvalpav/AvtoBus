# AvtoBus.EventSourcing — Полная реализация

> **Code sketch / unverified.** Это проектный прототип API и SQL, не проверенный Event Store. Канонический статус: [`../FINAL.md`](../FINAL.md).

Event Store на PostgreSQL, агрегаты, проекции, snapshots, upcasters.

---

## AvtoBus.EventSourcing/StoredEvent.cs

```csharp
namespace AvtoBus.EventSourcing;

/// <summary>
/// Событие в сторе.
/// </summary>
public sealed record StoredEvent
{
    public required long GlobalSequence { get; init; }
    public required Guid StreamId { get; init; }
    public required string StreamType { get; init; }
    public required int Version { get; init; }
    public required string EventType { get; init; }
    public required int SchemaVersion { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }
    public ReadOnlyMemory<byte> Metadata { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public Guid? CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public string? TenantId { get; init; }
    public string? PrevHash { get; init; }
}

/// <summary>
/// Событие для записи (ещё без GlobalSequence).
/// </summary>
public sealed record EventToAppend
{
    public required object Payload { get; init; }
    public required string EventType { get; init; }
    public int SchemaVersion { get; init; } = 1;
    public Dictionary<string, string> Metadata { get; init; } = new();
}

/// <summary>
/// Снапшот состояния агрегата.
/// </summary>
public sealed record StoredSnapshot
{
    public required Guid StreamId { get; init; }
    public required int Version { get; init; }
    public required string StateType { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
}

public sealed record StreamMetadata(
    Guid StreamId,
    string StreamType,
    int Version,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    bool IsArchived);
```

---

## AvtoBus.EventSourcing/IEventStore.cs

```csharp
namespace AvtoBus.EventSourcing;

public interface IEventStore
{
    /// <summary>
    /// Дописать события в стрим с проверкой ожидаемой версии (optimistic concurrency).
    /// expectedVersion = -1 → любая; 0 → стрим должен быть новым.
    /// </summary>
    ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        string streamType,
        IReadOnlyList<EventToAppend> events,
        int expectedVersion = -1,
        CancellationToken ct = default);

    /// <summary>Прочитать события стрима.</summary>
    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        Guid streamId,
        int fromVersion = 0,
        int? toVersion = null,
        CancellationToken ct = default);

    /// <summary>Прочитать глобальный поток (для проекций).</summary>
    IAsyncEnumerable<StoredEvent> ReadAllAsync(
        long fromSequence = 0,
        int batchSize = 1000,
        IReadOnlyList<string>? eventTypeFilter = null,
        CancellationToken ct = default);

    /// <summary>Прочитать события категории ($ce-orders).</summary>
    IAsyncEnumerable<StoredEvent> ReadCategoryAsync(
        string streamType,
        long fromSequence = 0,
        CancellationToken ct = default);

    ValueTask<StreamMetadata?> GetStreamAsync(Guid streamId, CancellationToken ct = default);
    ValueTask<long> GetHeadSequenceAsync(CancellationToken ct = default);

    ValueTask SaveSnapshotAsync(StoredSnapshot snapshot, CancellationToken ct = default);
    ValueTask<StoredSnapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default);

    ValueTask ArchiveStreamAsync(Guid streamId, CancellationToken ct = default);
}

public sealed record AppendResult(int NewVersion, long FirstSequence, long LastSequence);

public sealed class ConcurrencyException : Exception
{
    public Guid StreamId { get; }
    public int ExpectedVersion { get; }
    public int ActualVersion { get; }

    public ConcurrencyException(Guid streamId, int expected, int actual)
        : base($"Concurrency conflict on stream {streamId}: expected v{expected}, actual v{actual}")
        => (StreamId, ExpectedVersion, ActualVersion) = (streamId, expected, actual);
}
```

---

## AvtoBus.EventSourcing/Postgres/PostgresEventStore.cs

```csharp
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.EventSourcing.Postgres;

/// <summary>
/// Event Store на PostgreSQL.
/// Схема: events (global_seq BIGSERIAL, stream_id, version, ...) + unique(stream_id, version).
/// </summary>
public sealed class PostgresEventStore : IEventStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly TimeProvider _clock;
    private readonly ILogger<PostgresEventStore> _log;

    public PostgresEventStore(
        NpgsqlDataSource dataSource,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        TimeProvider clock,
        ILogger<PostgresEventStore> log)
    {
        _dataSource = dataSource;
        _serializer = serializer;
        _upcasters = upcasters;
        _clock = clock;
        _log = log;
    }

    // ── Append ──

    public async ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        string streamType,
        IReadOnlyList<EventToAppend> events,
        int expectedVersion = -1,
        CancellationToken ct = default)
    {
        if (events.Count == 0)
            throw new ArgumentException("No events to append", nameof(events));

        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);

        // 1. Заблокировать стрим и получить текущую версию
        var currentVersion = await GetStreamVersionForUpdateAsync(conn, tx, streamId, ct);

        if (expectedVersion >= 0 && currentVersion != expectedVersion)
            throw new ConcurrencyException(streamId, expectedVersion, currentVersion);

        // 2. Вставить события батчем
        var now = _clock.GetUtcNow();
        var version = currentVersion;
        var sequences = new List<long>(events.Count);

        await using var batch = new NpgsqlBatch(conn, tx);

        foreach (var e in events)
        {
            version++;
            var cmd = new NpgsqlBatchCommand("""
                INSERT INTO avtobus_events
                    (stream_id, stream_type, version, event_type, schema_version,
                     data, metadata, timestamp, correlation_id, causation_id, tenant_id)
                VALUES (@stream_id, @stream_type, @version, @event_type, @schema_version,
                        @data, @metadata, @timestamp, @correlation_id, @causation_id, @tenant_id)
                RETURNING global_seq
                """);

            cmd.Parameters.AddWithValue("stream_id", streamId);
            cmd.Parameters.AddWithValue("stream_type", streamType);
            cmd.Parameters.AddWithValue("version", version);
            cmd.Parameters.AddWithValue("event_type", e.EventType);
            cmd.Parameters.AddWithValue("schema_version", e.SchemaVersion);
            cmd.Parameters.AddWithValue("data", NpgsqlDbType.Bytea, _serializer.Serialize(e.Payload).ToArray());
            cmd.Parameters.AddWithValue("metadata", NpgsqlDbType.Jsonb,
                System.Text.Json.JsonSerializer.Serialize(e.Metadata));
            cmd.Parameters.AddWithValue("timestamp", now);
            cmd.Parameters.AddWithValue("correlation_id",
                (object?)ParseGuid(e.Metadata.GetValueOrDefault("correlationId")) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("causation_id",
                (object?)ParseGuid(e.Metadata.GetValueOrDefault("causationId")) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("tenant_id",
                (object?)e.Metadata.GetValueOrDefault("tenantId") ?? DBNull.Value);

            batch.BatchCommands.Add(cmd);
        }

        await using (var reader = await batch.ExecuteReaderAsync(ct))
        {
            do
            {
                while (await reader.ReadAsync(ct))
                    sequences.Add(reader.GetInt64(0));
            } while (await reader.NextResultAsync(ct));
        }

        // 3. Обновить метаданные стрима
        await UpsertStreamAsync(conn, tx, streamId, streamType, version, now, ct);

        await tx.CommitAsync(ct);

        _log.LogDebug("Appended {Count} events to stream {StreamId}, new version {Version}",
            events.Count, streamId, version);

        return new AppendResult(version, sequences.First(), sequences.Last());
    }

    private static async Task<int> GetStreamVersionForUpdateAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, Guid streamId, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT version FROM avtobus_streams WHERE stream_id = @id FOR UPDATE", conn, tx);
        cmd.Parameters.AddWithValue("id", streamId);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is int v ? v : 0;
    }

    private static async Task UpsertStreamAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx,
        Guid streamId, string streamType, int version, DateTimeOffset now, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_streams (stream_id, stream_type, version, created_at, updated_at)
            VALUES (@id, @type, @version, @now, @now)
            ON CONFLICT (stream_id) DO UPDATE
            SET version = @version, updated_at = @now
            """, conn, tx);
        cmd.Parameters.AddWithValue("id", streamId);
        cmd.Parameters.AddWithValue("type", streamType);
        cmd.Parameters.AddWithValue("version", version);
        cmd.Parameters.AddWithValue("now", now);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // ── Read stream ──

    public async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        Guid streamId,
        int fromVersion = 0,
        int? toVersion = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT global_seq, stream_id, stream_type, version, event_type, schema_version,
                   data, metadata, timestamp, correlation_id, causation_id, tenant_id
            FROM avtobus_events
            WHERE stream_id = @id AND version > @from
              AND (@to::int IS NULL OR version <= @to)
            ORDER BY version
            """, conn);

        cmd.Parameters.AddWithValue("id", streamId);
        cmd.Parameters.AddWithValue("from", fromVersion);
        cmd.Parameters.AddWithValue("to", (object?)toVersion ?? DBNull.Value);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            yield return MapEvent(reader);
    }

    // ── Read all (для проекций) ──

    public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
        long fromSequence = 0,
        int batchSize = 1000,
        IReadOnlyList<string>? eventTypeFilter = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var cursor = fromSequence;

        while (!ct.IsCancellationRequested)
        {
            var batch = new List<StoredEvent>(batchSize);

            await using (var conn = await _dataSource.OpenConnectionAsync(ct))
            await using (var cmd = new NpgsqlCommand("""
                SELECT global_seq, stream_id, stream_type, version, event_type, schema_version,
                       data, metadata, timestamp, correlation_id, causation_id, tenant_id
                FROM avtobus_events
                WHERE global_seq > @cursor
                  AND (@types::text[] IS NULL OR event_type = ANY(@types))
                ORDER BY global_seq
                LIMIT @limit
                """, conn))
            {
                cmd.Parameters.AddWithValue("cursor", cursor);
                cmd.Parameters.AddWithValue("types",
                    (object?)eventTypeFilter?.ToArray() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("limit", batchSize);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    batch.Add(MapEvent(reader));
            }

            if (batch.Count == 0)
                yield break;

            foreach (var e in batch)
            {
                cursor = e.GlobalSequence;
                yield return e;
            }
        }
    }

    public async IAsyncEnumerable<StoredEvent> ReadCategoryAsync(
        string streamType,
        long fromSequence = 0,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT global_seq, stream_id, stream_type, version, event_type, schema_version,
                   data, metadata, timestamp, correlation_id, causation_id, tenant_id
            FROM avtobus_events
            WHERE stream_type = @type AND global_seq > @cursor
            ORDER BY global_seq
            """, conn);
        cmd.Parameters.AddWithValue("type", streamType);
        cmd.Parameters.AddWithValue("cursor", fromSequence);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            yield return MapEvent(reader);
    }

    // ── Snapshots ──

    public async ValueTask SaveSnapshotAsync(StoredSnapshot snapshot, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_snapshots (stream_id, version, state_type, data, created_at)
            VALUES (@id, @version, @type, @data, @created)
            ON CONFLICT (stream_id) DO UPDATE
            SET version = @version, data = @data, created_at = @created, state_type = @type
            """, conn);
        cmd.Parameters.AddWithValue("id", snapshot.StreamId);
        cmd.Parameters.AddWithValue("version", snapshot.Version);
        cmd.Parameters.AddWithValue("type", snapshot.StateType);
        cmd.Parameters.AddWithValue("data", NpgsqlDbType.Bytea, snapshot.Data.ToArray());
        cmd.Parameters.AddWithValue("created", snapshot.CreatedAt);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask<StoredSnapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT stream_id, version, state_type, data, created_at FROM avtobus_snapshots WHERE stream_id = @id", conn);
        cmd.Parameters.AddWithValue("id", streamId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;

        return new StoredSnapshot
        {
            StreamId = reader.GetGuid(0),
            Version = reader.GetInt32(1),
            StateType = reader.GetString(2),
            Data = (byte[])reader[3],
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(4),
        };
    }

    public async ValueTask<StreamMetadata?> GetStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT stream_id, stream_type, version, created_at, updated_at, is_archived
            FROM avtobus_streams WHERE stream_id = @id
            """, conn);
        cmd.Parameters.AddWithValue("id", streamId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;

        return new StreamMetadata(
            reader.GetGuid(0), reader.GetString(1), reader.GetInt32(2),
            reader.GetFieldValue<DateTimeOffset>(3), reader.GetFieldValue<DateTimeOffset>(4),
            reader.GetBoolean(5));
    }

    public async ValueTask<long> GetHeadSequenceAsync(CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT COALESCE(MAX(global_seq), 0) FROM avtobus_events", conn);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    public async ValueTask ArchiveStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE avtobus_streams SET is_archived = TRUE WHERE stream_id = @id", conn);
        cmd.Parameters.AddWithValue("id", streamId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private StoredEvent MapEvent(NpgsqlDataReader reader) => new()
    {
        GlobalSequence = reader.GetInt64(0),
        StreamId = reader.GetGuid(1),
        StreamType = reader.GetString(2),
        Version = reader.GetInt32(3),
        EventType = reader.GetString(4),
        SchemaVersion = reader.GetInt32(5),
        Data = (byte[])reader[6],
        Metadata = System.Text.Encoding.UTF8.GetBytes(reader.GetString(7)),
        Timestamp = reader.GetFieldValue<DateTimeOffset>(8),
        CorrelationId = reader.IsDBNull(9) ? null : reader.GetGuid(9),
        CausationId = reader.IsDBNull(10) ? null : reader.GetGuid(10),
        TenantId = reader.IsDBNull(11) ? null : reader.GetString(11),
    };

    private static Guid? ParseGuid(string? s) => Guid.TryParse(s, out var g) ? g : null;
}
```

---

## AvtoBus.EventSourcing/Postgres/Schema.sql

```sql
-- Основная таблица событий
CREATE TABLE IF NOT EXISTS avtobus_events (
    global_seq      BIGSERIAL PRIMARY KEY,
    stream_id       UUID        NOT NULL,
    stream_type     TEXT        NOT NULL,
    version         INT         NOT NULL,
    event_type      TEXT        NOT NULL,
    schema_version  INT         NOT NULL DEFAULT 1,
    data            BYTEA       NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    timestamp       TIMESTAMPTZ NOT NULL,
    correlation_id  UUID        NULL,
    causation_id    UUID        NULL,
    tenant_id       TEXT        NULL,
    prev_hash       TEXT        NULL,

    CONSTRAINT uq_stream_version UNIQUE (stream_id, version)
);

CREATE INDEX IF NOT EXISTS ix_events_stream    ON avtobus_events (stream_id, version);
CREATE INDEX IF NOT EXISTS ix_events_type      ON avtobus_events (event_type, global_seq);
CREATE INDEX IF NOT EXISTS ix_events_category  ON avtobus_events (stream_type, global_seq);
CREATE INDEX IF NOT EXISTS ix_events_tenant    ON avtobus_events (tenant_id, global_seq) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_events_correlation ON avtobus_events (correlation_id) WHERE correlation_id IS NOT NULL;

-- Метаданные стримов
CREATE TABLE IF NOT EXISTS avtobus_streams (
    stream_id    UUID PRIMARY KEY,
    stream_type  TEXT        NOT NULL,
    version      INT         NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    is_archived  BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_streams_type ON avtobus_streams (stream_type);

-- Снапшоты
CREATE TABLE IF NOT EXISTS avtobus_snapshots (
    stream_id   UUID PRIMARY KEY,
    version     INT         NOT NULL,
    state_type  TEXT        NOT NULL,
    data        BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

-- Чекпоинты проекций
CREATE TABLE IF NOT EXISTS avtobus_projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    position        BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'running',
    last_error      TEXT        NULL
);
```

---

## AvtoBus.EventSourcing/Aggregate.cs

```csharp
namespace AvtoBus.EventSourcing;

/// <summary>
/// Базовый агрегат: накапливает несохранённые события.
/// </summary>
public abstract class Aggregate
{
    private readonly List<EventToAppend> _uncommitted = new();

    public Guid Id { get; protected set; }
    public int Version { get; internal set; }

    public IReadOnlyList<EventToAppend> UncommittedEvents => _uncommitted;

    /// <summary>Применить новое событие: изменить состояние и запомнить для записи.</summary>
    protected void Apply(object @event)
    {
        When(@event);
        _uncommitted.Add(new EventToAppend
        {
            Payload = @event,
            EventType = MessageTypeNaming.For(@event.GetType()),
            SchemaVersion = SchemaVersionOf(@event.GetType()),
        });
    }

    /// <summary>Восстановление из истории — только меняем состояние.</summary>
    internal void Replay(object @event)
    {
        When(@event);
        Version++;
    }

    /// <summary>Переопределить: применение события к состоянию.</summary>
    protected abstract void When(object @event);

    internal void MarkCommitted() => _uncommitted.Clear();

    private static int SchemaVersionOf(Type type) =>
        type.GetCustomAttributes(typeof(SchemaVersionAttribute), false)
            .OfType<SchemaVersionAttribute>()
            .FirstOrDefault()?.Version ?? 1;
}

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class SchemaVersionAttribute(int version) : Attribute
{
    public int Version { get; } = version;
}
```

---

## AvtoBus.EventSourcing/Decider.cs

```csharp
namespace AvtoBus.EventSourcing;

/// <summary>
/// Функциональный стиль (Decider pattern): чистые функции Decide + Evolve.
/// Максимально тестируемо — ни DI, ни IO.
/// </summary>
public interface IDecider<TState, TCommand, TEvent>
    where TState : class
    where TCommand : class
    where TEvent : class
{
    TState Initial { get; }
    IEnumerable<TEvent> Decide(TState state, TCommand command);
    TState Evolve(TState state, TEvent @event);
    bool IsTerminal(TState state) => false;
}

/// <summary>
/// Раннер для Decider: загрузка → решение → запись.
/// </summary>
public sealed class DeciderRunner<TState, TCommand, TEvent>
    where TState : class
    where TCommand : class
    where TEvent : class
{
    private readonly IDecider<TState, TCommand, TEvent> _decider;
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly string _streamType;

    public DeciderRunner(
        IDecider<TState, TCommand, TEvent> decider,
        IEventStore store,
        IEventSerializer serializer,
        string streamType)
    {
        _decider = decider;
        _store = store;
        _serializer = serializer;
        _streamType = streamType;
    }

    public async ValueTask<AppendResult> HandleAsync(
        Guid streamId,
        TCommand command,
        CancellationToken ct = default)
    {
        // 1. Восстановить состояние
        var state = _decider.Initial;
        var version = 0;

        await foreach (var stored in _store.ReadStreamAsync(streamId, ct: ct))
        {
            var @event = (TEvent)_serializer.Deserialize(stored.Data, stored.EventType);
            state = _decider.Evolve(state, @event);
            version = stored.Version;
        }

        // 2. Принять решение
        var newEvents = _decider.Decide(state, command).ToList();
        if (newEvents.Count == 0)
            return new AppendResult(version, 0, 0);

        // 3. Записать
        var toAppend = newEvents.Select(e => new EventToAppend
        {
            Payload = e,
            EventType = MessageTypeNaming.For(e.GetType()),
        }).ToList();

        return await _store.AppendAsync(streamId, _streamType, toAppend, version, ct);
    }
}
```

---

## AvtoBus.EventSourcing/IAggregateRepository.cs

```csharp
namespace AvtoBus.EventSourcing;

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
        var aggregate = new TAggregate();
        var fromVersion = 0;

        // 1. Попробовать снапшот
        var snapshot = await _store.LoadSnapshotAsync(id, ct);
        if (snapshot is not null && snapshot.StateType == typeof(TAggregate).FullName)
        {
            aggregate = _serializer.DeserializeSnapshot<TAggregate>(snapshot.Data);
            aggregate.Version = snapshot.Version;
            fromVersion = snapshot.Version;
        }

        // 2. Догнать хвост событий
        var found = fromVersion > 0;
        await foreach (var stored in _store.ReadStreamAsync(id, fromVersion, ct: ct))
        {
            found = true;
            var @event = Upcast(stored);
            aggregate.Replay(@event);
        }

        return found ? aggregate : null;
    }

    public async ValueTask<TAggregate> LoadOrCreateAsync<TAggregate>(Guid id, CancellationToken ct = default)
        where TAggregate : Aggregate, new()
        => await LoadAsync<TAggregate>(id, ct) ?? new TAggregate();

    public async ValueTask<TAggregate?> LoadAsOfAsync<TAggregate>(
        Guid id, DateTimeOffset asOf, CancellationToken ct = default)
        where TAggregate : Aggregate, new()
    {
        var aggregate = new TAggregate();
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

        // Публикация событий в шину (через outbox, если он активен)
        if (_bus is not null)
        {
            foreach (var e in events)
                await _bus.Publish(e.Payload, new PublishOptions
                {
                    PartitionKey = aggregate.Id.ToString(),
                }, ct);
        }

        aggregate.Version = result.NewVersion;
        aggregate.MarkCommitted();

        // Снапшот по политике
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
```

---

## AvtoBus.EventSourcing/Upcaster.cs

```csharp
namespace AvtoBus.EventSourcing;

/// <summary>
/// Преобразование события старой версии схемы в новую (Axon-подход).
/// </summary>
public interface IUpcaster
{
    string EventType { get; }
    int FromVersion { get; }
    int ToVersion { get; }
    object Upcast(object oldEvent);
}

public abstract class Upcaster<TOld, TNew> : IUpcaster
    where TOld : class where TNew : class
{
    public abstract string EventType { get; }
    public abstract int FromVersion { get; }
    public virtual int ToVersion => FromVersion + 1;

    public abstract TNew Upcast(TOld old);

    object IUpcaster.Upcast(object oldEvent) => Upcast((TOld)oldEvent);
}

/// <summary>
/// Цепочка upcaster-ов: v1 → v2 → v3 применяется автоматически при чтении.
/// </summary>
public sealed class UpcasterChain
{
    private readonly Dictionary<(string EventType, int Version), IUpcaster> _chain;

    public UpcasterChain(IEnumerable<IUpcaster> upcasters)
    {
        _chain = upcasters.ToDictionary(u => (u.EventType, u.FromVersion));
    }

    public object Upcast(object @event, string eventType, int schemaVersion)
    {
        var current = @event;
        var version = schemaVersion;

        while (_chain.TryGetValue((eventType, version), out var upcaster))
        {
            current = upcaster.Upcast(current);
            version = upcaster.ToVersion;
        }

        return current;
    }
}
```

Пример:

```csharp
public sealed class OrderPlacedV1ToV2 : Upcaster<OrderPlacedV1, OrderPlacedV2>
{
    public override string EventType => "order-placed";
    public override int FromVersion => 1;

    public override OrderPlacedV2 Upcast(OrderPlacedV1 old)
        => new(old.OrderId, old.Total, Currency: "RUB");  // новое поле с дефолтом
}
```

---

## AvtoBus.EventSourcing/Projections/IProjection.cs

```csharp
namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Проекция: строит read-модель из потока событий.
/// </summary>
public interface IProjection
{
    string Name { get; }
    IReadOnlyList<string> HandledEventTypes { get; }
    ProjectionMode Mode { get; }

    ValueTask ApplyAsync(StoredEvent stored, object @event, CancellationToken ct);
    ValueTask<long> GetCheckpointAsync(CancellationToken ct);
    ValueTask SaveCheckpointAsync(long position, CancellationToken ct);
    ValueTask ResetAsync(CancellationToken ct);
}

public enum ProjectionMode
{
    /// <summary>В транзакции записи — строгая согласованность.</summary>
    Inline,
    /// <summary>Фоновый daemon с чекпоинтами.</summary>
    Async,
    /// <summary>Считается на лету при чтении.</summary>
    Live,
}

/// <summary>
/// Базовый класс проекции с диспетчеризацией по типу события.
/// </summary>
public abstract class Projection : IProjection
{
    private readonly Dictionary<string, Func<StoredEvent, object, CancellationToken, ValueTask>> _handlers = new();

    public abstract string Name { get; }
    public virtual ProjectionMode Mode => ProjectionMode.Async;
    public IReadOnlyList<string> HandledEventTypes => _handlers.Keys.ToList();

    protected void On<TEvent>(Func<TEvent, CancellationToken, ValueTask> handler) where TEvent : class
    {
        var type = MessageTypeNaming.For(typeof(TEvent));
        _handlers[type] = (_, e, ct) => handler((TEvent)e, ct);
    }

    protected void On<TEvent>(Func<TEvent, StoredEvent, CancellationToken, ValueTask> handler) where TEvent : class
    {
        var type = MessageTypeNaming.For(typeof(TEvent));
        _handlers[type] = (stored, e, ct) => handler((TEvent)e, stored, ct);
    }

    public ValueTask ApplyAsync(StoredEvent stored, object @event, CancellationToken ct)
        => _handlers.TryGetValue(stored.EventType, out var handler)
            ? handler(stored, @event, ct)
            : ValueTask.CompletedTask;

    public abstract ValueTask<long> GetCheckpointAsync(CancellationToken ct);
    public abstract ValueTask SaveCheckpointAsync(long position, CancellationToken ct);
    public abstract ValueTask ResetAsync(CancellationToken ct);
}
```

---

## AvtoBus.EventSourcing/Projections/ProjectionDaemon.cs

```csharp
using Microsoft.Extensions.Hosting;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Фоновый обработчик async-проекций: читает global-поток, применяет, чекпоинтит.
/// </summary>
public sealed class ProjectionDaemon : BackgroundService
{
    private readonly IEventStore _store;
    private readonly IEnumerable<IProjection> _projections;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly ProjectionDaemonOptions _options;
    private readonly ILogger<ProjectionDaemon> _log;

    public ProjectionDaemon(
        IEventStore store,
        IEnumerable<IProjection> projections,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        ProjectionDaemonOptions options,
        ILogger<ProjectionDaemon> log)
    {
        _store = store;
        _projections = projections.Where(p => p.Mode == ProjectionMode.Async).ToList();
        _serializer = serializer;
        _upcasters = upcasters;
        _options = options;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tasks = _projections.Select(p => RunProjectionAsync(p, ct));
        await Task.WhenAll(tasks);
    }

    private async Task RunProjectionAsync(IProjection projection, CancellationToken ct)
    {
        _log.LogInformation("Projection {Name} starting", projection.Name);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var checkpoint = await projection.GetCheckpointAsync(ct);
                var head = await _store.GetHeadSequenceAsync(ct);
                var lag = head - checkpoint;

                ProjectionMetrics.Lag.Record(lag, new TagList { { "projection", projection.Name } });

                if (lag == 0)
                {
                    await Task.Delay(_options.IdleDelay, ct);
                    continue;
                }

                var processed = 0;
                var lastPosition = checkpoint;

                await foreach (var stored in _store.ReadAllAsync(
                    checkpoint, _options.BatchSize,
                    projection.HandledEventTypes.Count > 0 ? projection.HandledEventTypes : null,
                    ct))
                {
                    var @event = _upcasters.Upcast(
                        _serializer.Deserialize(stored.Data, stored.EventType),
                        stored.EventType, stored.SchemaVersion);

                    await projection.ApplyAsync(stored, @event, ct);

                    lastPosition = stored.GlobalSequence;
                    processed++;

                    if (processed >= _options.BatchSize) break;
                }

                if (processed > 0)
                {
                    await projection.SaveCheckpointAsync(lastPosition, ct);
                    _log.LogDebug("Projection {Name}: applied {Count} events, position {Pos}",
                        projection.Name, processed, lastPosition);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Projection {Name} failed, retrying in {Delay}",
                    projection.Name, _options.ErrorDelay);
                await Task.Delay(_options.ErrorDelay, ct);
            }
        }
    }
}

public sealed class ProjectionDaemonOptions
{
    public int BatchSize { get; set; } = 500;
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);
}

internal static class ProjectionMetrics
{
    public static readonly Histogram<long> Lag =
        BusMetrics.Meter.CreateHistogram<long>("avtobus.projection.lag", "events");
}
```

---

## AvtoBus.EventSourcing/Projections/PostgresProjectionCheckpoints.cs

```csharp
using Npgsql;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Базовый класс проекции с чекпоинтом в PostgreSQL.
/// </summary>
public abstract class PostgresProjection : Projection
{
    protected readonly NpgsqlDataSource DataSource;

    protected PostgresProjection(NpgsqlDataSource dataSource) => DataSource = dataSource;

    public override async ValueTask<long> GetCheckpointAsync(CancellationToken ct)
    {
        await using var conn = await DataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT position FROM avtobus_projection_checkpoints WHERE projection_name = @name", conn);
        cmd.Parameters.AddWithValue("name", Name);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is long p ? p : 0;
    }

    public override async ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
    {
        await using var conn = await DataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_projection_checkpoints (projection_name, position, updated_at, status)
            VALUES (@name, @pos, now(), 'running')
            ON CONFLICT (projection_name) DO UPDATE
            SET position = @pos, updated_at = now(), status = 'running', last_error = NULL
            """, conn);
        cmd.Parameters.AddWithValue("name", Name);
        cmd.Parameters.AddWithValue("pos", position);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override async ValueTask ResetAsync(CancellationToken ct)
    {
        await using var conn = await DataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            UPDATE avtobus_projection_checkpoints SET position = 0, updated_at = now()
            WHERE projection_name = @name
            """, conn);
        cmd.Parameters.AddWithValue("name", Name);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

Пример проекции:

```csharp
public sealed class CustomerLtvProjection : PostgresProjection
{
    public override string Name => "customer-ltv";

    public CustomerLtvProjection(NpgsqlDataSource ds) : base(ds)
    {
        On<OrderPaid>(async (e, ct) =>
        {
            await using var conn = await DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand("""
                INSERT INTO customer_ltv (customer_id, total, orders)
                VALUES (@id, @amount, 1)
                ON CONFLICT (customer_id) DO UPDATE
                SET total = customer_ltv.total + @amount, orders = customer_ltv.orders + 1
                """, conn);
            cmd.Parameters.AddWithValue("id", e.CustomerId);
            cmd.Parameters.AddWithValue("amount", e.Amount);
            await cmd.ExecuteNonQueryAsync(ct);
        });

        On<OrderRefunded>(async (e, ct) => { /* вычесть */ });
    }
}
```

---

## AvtoBus.EventSourcing/IEventSerializer.cs

```csharp
namespace AvtoBus.EventSourcing;

public interface IEventSerializer
{
    ReadOnlyMemory<byte> Serialize(object @event);
    object Deserialize(ReadOnlyMemory<byte> data, string eventType);
    ReadOnlyMemory<byte> SerializeSnapshot(object state);
    T DeserializeSnapshot<T>(ReadOnlyMemory<byte> data) where T : class;
    void RegisterType(string eventType, Type clrType);
}

public sealed class JsonEventSerializer : IEventSerializer
{
    private readonly Dictionary<string, Type> _typeMap = new(StringComparer.OrdinalIgnoreCase);
    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(IEnumerable<Type> eventTypes, JsonSerializerOptions? options = null)
    {
        _options = options ?? DefaultJsonSerializer.CreateDefaultOptions();
        foreach (var t in eventTypes)
            _typeMap[MessageTypeNaming.For(t)] = t;
    }

    public void RegisterType(string eventType, Type clrType) => _typeMap[eventType] = clrType;

    public ReadOnlyMemory<byte> Serialize(object @event)
        => JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _options);

    public object Deserialize(ReadOnlyMemory<byte> data, string eventType)
    {
        if (!_typeMap.TryGetValue(eventType, out var clrType))
            throw new UnknownEventTypeException(eventType);

        return JsonSerializer.Deserialize(data.Span, clrType, _options)
            ?? throw new SerializationException($"Null after deserializing {eventType}");
    }

    public ReadOnlyMemory<byte> SerializeSnapshot(object state)
        => JsonSerializer.SerializeToUtf8Bytes(state, state.GetType(), _options);

    public T DeserializeSnapshot<T>(ReadOnlyMemory<byte> data) where T : class
        => JsonSerializer.Deserialize<T>(data.Span, _options)!;
}

public sealed class UnknownEventTypeException(string eventType)
    : Exception($"Unknown event type '{eventType}'. Register it in JsonEventSerializer.");
```

---

## AvtoBus.EventSourcing/Registration.cs

```csharp
namespace AvtoBus;

public static class EventSourcingRegistration
{
    public static BusOptions UseEventSourcing(
        this BusOptions bus,
        string connectionString,
        Action<EventSourcingOptions>? configure = null)
    {
        var options = new EventSourcingOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(options);
        bus.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
        bus.Services.AddSingleton<IEventStore, PostgresEventStore>();
        bus.Services.AddSingleton<IAggregateRepository, AggregateRepository>();
        bus.Services.AddSingleton(options.SnapshotPolicy);
        bus.Services.AddSingleton<IEventSerializer>(sp =>
            new JsonEventSerializer(options.EventTypes));
        bus.Services.AddSingleton(sp =>
            new UpcasterChain(sp.GetServices<IUpcaster>()));
        bus.Services.AddSingleton(new ProjectionDaemonOptions());
        bus.Services.AddHostedService<ProjectionDaemon>();

        foreach (var projectionType in options.Projections)
            bus.Services.AddSingleton(typeof(IProjection), projectionType);

        return bus;
    }
}

public sealed class EventSourcingOptions
{
    public List<Type> EventTypes { get; } = new();
    public List<Type> Projections { get; } = new();
    public SnapshotPolicy SnapshotPolicy { get; } = new();

    public EventSourcingOptions AddEventsFromAssembly(Assembly assembly)
    {
        EventTypes.AddRange(assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } && t.Name.EndsWith("Event") ||
                        t.GetCustomAttributes(typeof(MessageAliasAttribute), false).Length > 0));
        return this;
    }

    public EventSourcingOptions AddProjection<TProjection>() where TProjection : IProjection
    {
        Projections.Add(typeof(TProjection));
        return this;
    }

    public EventSourcingOptions SnapshotEvery<TAggregate>(int events) where TAggregate : Aggregate
    {
        SnapshotPolicy.For<TAggregate>(events);
        return this;
    }
}
```
