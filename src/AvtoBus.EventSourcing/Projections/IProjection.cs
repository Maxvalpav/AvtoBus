namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Проекция: строит read-модель из потока событий (идея 254).
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
        var type = MessageTypeNaming.NameOf(typeof(TEvent));
        _handlers[type] = (_, e, ct) => handler((TEvent)e, ct);
    }

    protected void On<TEvent>(Func<TEvent, StoredEvent, CancellationToken, ValueTask> handler) where TEvent : class
    {
        var type = MessageTypeNaming.NameOf(typeof(TEvent));
        _handlers[type] = (stored, e, ct) => handler((TEvent)e, stored, ct);
    }

    public virtual ValueTask ApplyAsync(StoredEvent stored, object @event, CancellationToken ct)
        => _handlers.TryGetValue(stored.EventType, out var handler)
            ? handler(stored, @event, ct)
            : ValueTask.CompletedTask;

    public abstract ValueTask<long> GetCheckpointAsync(CancellationToken ct);

    public abstract ValueTask SaveCheckpointAsync(long position, CancellationToken ct);

    public abstract ValueTask ResetAsync(CancellationToken ct);
}
