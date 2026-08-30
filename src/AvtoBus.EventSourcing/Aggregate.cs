namespace AvtoBus.EventSourcing;

/// <summary>
/// Базовый агрегат: накапливает несохранённые события и умеет восстанавливаться из истории.
/// </summary>
public abstract class Aggregate
{
    private readonly List<EventToAppend> _uncommitted = new();

    public Guid Id { get; set; }

    public int Version { get; internal set; }

    public IReadOnlyList<EventToAppend> UncommittedEvents => _uncommitted;

    /// <summary>Применить новое событие: изменить состояние и запомнить для записи.</summary>
    protected void Apply(object @event)
    {
        When(@event);
        _uncommitted.Add(new EventToAppend
        {
            Payload = @event,
            EventType = MessageTypeNaming.NameOf(@event.GetType()),
            SchemaVersion = SchemaVersionOf(@event.GetType()),
        });
    }

    /// <summary>Восстановление из истории — только меняет состояние.</summary>
    internal void Replay(object @event)
    {
        When(@event);
        Version++;
    }

    /// <summary>Применение события к состоянию агрегата.</summary>
    protected abstract void When(object @event);

    internal void MarkCommitted() => _uncommitted.Clear();

    private static int SchemaVersionOf(Type type)
        => type.GetCustomAttributes(typeof(SchemaVersionAttribute), false)
            .OfType<SchemaVersionAttribute>()
            .FirstOrDefault()?.Version ?? 1;
}

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class SchemaVersionAttribute(int version) : Attribute
{
    public int Version { get; } = version;
}
