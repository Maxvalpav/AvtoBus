using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.EventSourcing;

/// <summary>
/// Event Store на EF Core (пользовательский <typeparamref name="TDb"/> + <c>ConfigureEventSourcing</c>).
/// Optimistic concurrency: уникальный индекс (stream_id, version) + перехват DbUpdateException.
/// </summary>
public sealed class EfCoreEventStore<TDb> : IEventStore
    where TDb : DbContext
{
    private readonly IServiceScopeFactory _scopes;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly TimeProvider _clock;

    public EfCoreEventStore(
        IServiceScopeFactory scopes,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        TimeProvider clock)
    {
        _scopes = scopes;
        _serializer = serializer;
        _upcasters = upcasters;
        _clock = clock;
    }

    public async ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        string streamType,
        IReadOnlyList<EventToAppend> events,
        int expectedVersion = -1,
        CancellationToken ct = default)
    {
        if (events.Count == 0)
            throw new ArgumentException("No events to append", nameof(events));

        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var stream = await db.Set<EsStream>().FindAsync([streamId], ct);
        var currentVersion = stream?.Version ?? 0;

        if (expectedVersion >= 0 && currentVersion != expectedVersion)
            throw new ConcurrencyException(streamId, expectedVersion, currentVersion);
        if (expectedVersion == 0 && stream is not null)
            throw new ConcurrencyException(streamId, 0, currentVersion);

        var now = _clock.GetUtcNow();
        var version = currentVersion;
        var entities = new List<EsEvent>(events.Count);

        foreach (var e in events)
        {
            version++;
            entities.Add(new EsEvent
            {
                StreamId = streamId,
                StreamType = streamType,
                Version = version,
                EventType = e.EventType,
                SchemaVersion = e.SchemaVersion,
                Data = _serializer.Serialize(e.Payload).ToArray(),
                Metadata = SerializeMetadata(e.Metadata),
                Timestamp = now,
                CorrelationId = ParseGuid(e.Metadata.GetValueOrDefault("correlationId")),
                CausationId = ParseGuid(e.Metadata.GetValueOrDefault("causationId")),
                TenantId = e.Metadata.GetValueOrDefault("tenantId"),
            });
        }

        // Обе операции — события и метаданные стрима — в одной транзакции для консистентности.
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        try
        {
            db.Set<EsEvent>().AddRange(entities);

            if (stream is null)
                db.Set<EsStream>().Add(new EsStream
                {
                    StreamId = streamId,
                    StreamType = streamType,
                    Version = version,
                    CreatedAt = now,
                    UpdatedAt = now,
                });
            else
            {
                stream.Version = version;
                stream.UpdatedAt = now;
            }

            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            await tx.RollbackAsync(ct);
            throw new ConcurrencyException(streamId, expectedVersion, currentVersion);
        }

        var sequences = entities.Select(e => e.GlobalSequence).ToList();

        return new AppendResult(version, sequences[0], sequences[^1]);
    }

    public IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        Guid streamId, int fromVersion = 0, int? toVersion = null, CancellationToken ct = default)
        => ReadStreamCoreAsync(streamId, fromVersion, toVersion, ct);

    private async IAsyncEnumerable<StoredEvent> ReadStreamCoreAsync(
        Guid streamId, int fromVersion, int? toVersion,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        IQueryable<EsEvent> query = db.Set<EsEvent>()
            .Where(e => e.StreamId == streamId && e.Version > fromVersion)
            .OrderBy(e => e.Version);

        if (toVersion is int to)
            query = query.Where(e => e.Version <= to);

        await foreach (var e in query.AsNoTracking().AsAsyncEnumerable().WithCancellation(ct))
            yield return MapEvent(e);
    }

    public IAsyncEnumerable<StoredEvent> ReadAllAsync(
        long fromSequence, int batchSize = 1000, IReadOnlyList<string>? eventTypeFilter = null,
        CancellationToken ct = default)
        => ReadAllCoreAsync(fromSequence, batchSize, eventTypeFilter, ct);

    private async IAsyncEnumerable<StoredEvent> ReadAllCoreAsync(
        long fromSequence, int batchSize, IReadOnlyList<string>? eventTypeFilter,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var query = db.Set<EsEvent>().Where(e => e.GlobalSequence > fromSequence);
        if (eventTypeFilter is { Count: > 0 })
            query = query.Where(e => eventTypeFilter.Contains(e.EventType));

        await foreach (var e in query.OrderBy(e => e.GlobalSequence).Take(batchSize).AsNoTracking().AsAsyncEnumerable().WithCancellation(ct))
            yield return MapEvent(e);
    }

    public IAsyncEnumerable<StoredEvent> ReadCategoryAsync(
        string streamType, long fromSequence = 0, CancellationToken ct = default)
        => ReadCategoryCoreAsync(streamType, fromSequence, ct);

    private async IAsyncEnumerable<StoredEvent> ReadCategoryCoreAsync(
        string streamType, long fromSequence,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await foreach (var e in db.Set<EsEvent>()
                     .Where(x => x.StreamType == streamType && x.GlobalSequence > fromSequence)
                     .OrderBy(x => x.GlobalSequence)
                     .AsNoTracking().AsAsyncEnumerable().WithCancellation(ct))
            yield return MapEvent(e);
    }

    public async ValueTask<StreamMetadata?> GetStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();
        var s = await db.Set<EsStream>().AsNoTracking().FirstOrDefaultAsync(x => x.StreamId == streamId, ct);
        return s is null
            ? null
            : new StreamMetadata(s.StreamId, s.StreamType, s.Version, s.CreatedAt, s.UpdatedAt, s.IsArchived);
    }

    public async ValueTask<long> GetHeadSequenceAsync(CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();
        return await db.Set<EsEvent>().AnyAsync(ct)
            ? await db.Set<EsEvent>().MaxAsync(e => (long?)e.GlobalSequence, ct) ?? 0
            : 0;
    }

    public async ValueTask SaveSnapshotAsync(StoredSnapshot snapshot, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var existing = await db.Set<EsSnapshot>().FindAsync([snapshot.StreamId], ct);
        if (existing is null)
        {
            db.Set<EsSnapshot>().Add(new EsSnapshot
            {
                StreamId = snapshot.StreamId,
                Version = snapshot.Version,
                StateType = snapshot.StateType,
                Data = snapshot.Data.ToArray(),
                CreatedAt = snapshot.CreatedAt,
            });
        }
        else
        {
            existing.Version = snapshot.Version;
            existing.StateType = snapshot.StateType;
            existing.Data = snapshot.Data.ToArray();
            existing.CreatedAt = snapshot.CreatedAt;
        }

        await db.SaveChangesAsync(ct);
    }

    public async ValueTask<StoredSnapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();
        var s = await db.Set<EsSnapshot>().AsNoTracking().FirstOrDefaultAsync(x => x.StreamId == streamId, ct);
        return s is null
            ? null
            : new StoredSnapshot
            {
                StreamId = s.StreamId,
                Version = s.Version,
                StateType = s.StateType,
                Data = s.Data,
                CreatedAt = s.CreatedAt,
            };
    }

    public async ValueTask ArchiveStreamAsync(Guid streamId, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();
        var s = await db.Set<EsStream>().FirstOrDefaultAsync(x => x.StreamId == streamId, ct);
        if (s is not null) s.IsArchived = true;
        await db.SaveChangesAsync(ct);
    }

    private static StoredEvent MapEvent(EsEvent e) => new()
    {
        GlobalSequence = e.GlobalSequence,
        StreamId = e.StreamId,
        StreamType = e.StreamType,
        Version = e.Version,
        EventType = e.EventType,
        SchemaVersion = e.SchemaVersion,
        Data = e.Data,
        Metadata = e.Metadata,
        Timestamp = e.Timestamp,
        CorrelationId = e.CorrelationId,
        CausationId = e.CausationId,
        TenantId = e.TenantId,
        PrevHash = e.PrevHash,
    };

    private static byte[] SerializeMetadata(Dictionary<string, string> metadata)
        => metadata.Count == 0
            ? []
            : System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
                metadata, EventSourcingJsonContext.Default.DictionaryStringString);

    private static Guid? ParseGuid(string? s) => Guid.TryParse(s, out var g) ? g : null;

    private static bool IsUniqueConstraintViolation(DbUpdateException ex)
    {
        var inner = ex.InnerException;
        if (inner is null) return false;
        if (inner.GetType().Name == "PostgresException")
        {
            var sqlState = GetSqlState(inner);
            if (sqlState == "23505") return true; // any unique violation → concurrency (stream version)
        }
        // Fallback generic: SQLite / other providers
        return inner.Message.Contains("UNIQUE constraint failed", StringComparison.OrdinalIgnoreCase)
            || inner.Message.Contains("duplicate key", StringComparison.OrdinalIgnoreCase)
            || inner.Message.Contains("uq_stream", StringComparison.OrdinalIgnoreCase);
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075", Justification =
        "Провайдер-нейтральный сниффинг PostgresException.SqlState без зависимости на Npgsql; " +
        "читается только строковый код состояния, типы приложения не затрагиваются.")]
    private static string? GetSqlState(Exception inner)
        => inner.GetType().GetProperty("SqlState")?.GetValue(inner) as string;
}

/// <summary>Source-generated контекст для служебных словарей стора (аудит D5, trim-safe).</summary>
[JsonSerializable(typeof(Dictionary<string, string>))]
internal sealed partial class EventSourcingJsonContext : JsonSerializerContext
{
}
