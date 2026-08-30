using Microsoft.Extensions.Logging;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Управление проекциями: реплей, статусы/lag, blue/green переключение (идеи 259–261, 294).
/// </summary>
public interface IProjectionManager
{
    /// <summary>Список проекций и их отставание от головы стора.</summary>
    ValueTask<IReadOnlyList<ProjectionStatus>> GetStatusAsync(CancellationToken ct = default);

    /// <summary>Teardown, реплей с 0 и catch-up существующей проекции (идея 259).</summary>
    ValueTask RebuildAsync(string projectionName, CancellationToken ct = default);

    /// <summary>
    /// Blue/green: строит версию <paramref name="version"/> параллельно под чекпоинтом
    /// <c>name::v{version}</c> и дожидается catch-up. Требует <see cref="IVersionedProjection"/>.
    /// </summary>
    ValueTask BuildVersionAsync(string projectionName, int version, CancellationToken ct = default);

    /// <summary>Атомарно активирует построенную версию: переносит чекпоинт на основное имя.</summary>
    ValueTask ActivateVersionAsync(string projectionName, int version, CancellationToken ct = default);

    /// <summary>Удаляет чекпоинт версии (после того, как v1 выведена из эксплуатации).</summary>
    ValueTask DropVersionAsync(string projectionName, int version, CancellationToken ct = default);
}

public sealed record ProjectionStatus(string Name, long Position, long Head, long Lag, string State);

/// <summary>
/// Проекция, поддерживающая blue/green: read-модель и чекпоинт именуются по версии,
/// старая версия продолжает обслуживать чтение, пока новая не догонит голову.
/// </summary>
public interface IVersionedProjection
{
    ValueTask<long> GetCheckpointAsync(string checkpointName, CancellationToken ct);

    ValueTask SaveCheckpointAsync(string checkpointName, long position, CancellationToken ct);

    ValueTask ResetAsync(string checkpointName, CancellationToken ct);
}

/// <summary>
/// Реализация поверх <see cref="IEventStore"/>: общий движок реплея, чекпоинты по именам.
/// Blue/green требует, чтобы проекция реализовала <see cref="IVersionedProjection"/> — иначе
/// read-модель не может строиться параллельно без разрушения активной версии.
/// </summary>
public sealed class ProjectionManager : IProjectionManager
{
    private readonly IEventStore _store;
    private readonly IEnumerable<IProjection> _projections;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly ILogger<ProjectionManager> _log;

    public ProjectionManager(
        IEventStore store,
        IEnumerable<IProjection> projections,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        ILogger<ProjectionManager> log)
    {
        _store = store;
        _projections = projections;
        _serializer = serializer;
        _upcasters = upcasters;
        _log = log;
    }

    public async ValueTask<IReadOnlyList<ProjectionStatus>> GetStatusAsync(CancellationToken ct = default)
    {
        var head = await _store.GetHeadSequenceAsync(ct);
        var result = new List<ProjectionStatus>();

        foreach (var projection in _projections)
        {
            var position = await projection.GetCheckpointAsync(ct);
            result.Add(new ProjectionStatus(
                projection.Name,
                position,
                head,
                head - position,
                position >= head ? "caught-up" : "lagging"));
        }

        return result;
    }

    public async ValueTask RebuildAsync(string projectionName, CancellationToken ct = default)
    {
        var projection = Find(projectionName);
        await projection.ResetAsync(ct);
        _log.LogInformation("Projection {Name} reset, replaying from 0", projection.Name);

        await ReplayToHeadAsync(projection, projection.Name, ct);
    }

    public async ValueTask BuildVersionAsync(string projectionName, int version, CancellationToken ct = default)
    {
        var projection = Find(projectionName);
        var versioned = RequireVersioned(projection);
        var checkpointName = CheckpointName(projectionName, version);

        await versioned.ResetAsync(checkpointName, ct);
        _log.LogInformation("Projection {Name} building version v{Version} from 0", projectionName, version);

        await ReplayToHeadAsync(projection, checkpointName, ct);
    }

    public async ValueTask ActivateVersionAsync(string projectionName, int version, CancellationToken ct = default)
    {
        var projection = Find(projectionName);
        var versioned = RequireVersioned(projection);
        var checkpointName = CheckpointName(projectionName, version);

        var stagedPosition = await versioned.GetCheckpointAsync(checkpointName, ct);
        var head = await _store.GetHeadSequenceAsync(ct);

        // Не активируем недогнавшую версию: это была бы потеря событий.
        if (stagedPosition < head)
            throw new InvalidOperationException(
                $"Projection '{projectionName}' v{version} is behind head ({stagedPosition} < {head}); rebuild first");

        await projection.SaveCheckpointAsync(stagedPosition, ct);
        _log.LogInformation("Projection {Name} activated at v{Version} (position {Pos})",
            projectionName, version, stagedPosition);
    }

    public async ValueTask DropVersionAsync(string projectionName, int version, CancellationToken ct = default)
    {
        var projection = Find(projectionName);
        var versioned = RequireVersioned(projection);
        var checkpointName = CheckpointName(projectionName, version);

        await versioned.ResetAsync(checkpointName, ct);
        _log.LogInformation("Projection {Name} dropped version v{Version}", projectionName, version);
    }

    private IProjection Find(string projectionName)
    {
        var projection = _projections.FirstOrDefault(p => p.Name == projectionName);
        return projection ?? throw new ArgumentException(
            $"No projection named '{projectionName}'. Registered: {string.Join(", ", _projections.Select(p => p.Name))}",
            nameof(projectionName));
    }

    private static IVersionedProjection RequireVersioned(IProjection projection)
        => projection as IVersionedProjection ?? throw new NotSupportedException(
            $"Projection '{projection.Name}' does not implement {nameof(IVersionedProjection)} — " +
            "required for blue/green versioning. Implement it with a per-version read-model and checkpoint.");

    private static string CheckpointName(string projectionName, int version)
        => $"{projectionName}::v{version}";

    private async ValueTask ReplayToHeadAsync(
        IProjection projection, string checkpointName, CancellationToken ct)
    {
        const int batchSize = 500;
        var lastPosition = await ProjectionCheckpoint(
            projection, checkpointName, ct);
        var head = await _store.GetHeadSequenceAsync(ct);

        while (lastPosition < head && !ct.IsCancellationRequested)
        {
            var processed = 0;

            await foreach (var stored in _store.ReadAllAsync(
                lastPosition, batchSize,
                projection.HandledEventTypes.Count > 0 ? projection.HandledEventTypes : null,
                ct))
            {
                var @event = _upcasters.Upcast(
                    _serializer.Deserialize(stored.Data, stored.EventType),
                    stored.EventType, stored.SchemaVersion);

                await projection.ApplyAsync(stored, @event, ct);

                lastPosition = stored.GlobalSequence;
                processed++;
            }

            if (processed == 0)
                break;

            await SaveProjectionCheckpoint(projection, checkpointName, lastPosition, ct);
            _log.LogDebug("Projection {Name}: replayed {Count} events, position {Pos}",
                checkpointName, processed, lastPosition);
        }

        _log.LogInformation("Projection {Name}: caught up to {Head}", checkpointName, lastPosition);
    }

    private static ValueTask<long> ProjectionCheckpoint(
        IProjection projection, string checkpointName, CancellationToken ct)
        => projection is IVersionedProjection versioned
            ? versioned.GetCheckpointAsync(checkpointName, ct)
            : projection.GetCheckpointAsync(ct);

    private static ValueTask SaveProjectionCheckpoint(
        IProjection projection, string checkpointName, long position, CancellationToken ct)
        => projection is IVersionedProjection versioned
            ? versioned.SaveCheckpointAsync(checkpointName, position, ct)
            : projection.SaveCheckpointAsync(position, ct);
}
