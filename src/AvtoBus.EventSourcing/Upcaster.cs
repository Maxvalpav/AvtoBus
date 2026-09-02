namespace AvtoBus.EventSourcing;

/// <summary>Преобразование события старой версии схемы в новую (Axon-подход, идея 252).</summary>
public interface IUpcaster
{
    string EventType { get; }
    int FromVersion { get; }
    int ToVersion { get; }
    object Upcast(object oldEvent);
}

public abstract class Upcaster<TOld, TNew> : IUpcaster
    where TOld : class
    where TNew : class
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
        var visited = new HashSet<(string, int)>();

        while (_chain.TryGetValue((eventType, version), out var upcaster))
        {
            if (!visited.Add((eventType, version)))
                throw new InvalidOperationException($"Upcaster cycle detected at {eventType} v{version}");
            current = upcaster.Upcast(current);
            version = upcaster.ToVersion;
            if (visited.Count > 100)
                throw new InvalidOperationException($"Upcaster chain too long for {eventType} v{schemaVersion} — possible infinite loop");
        }

        return current;
    }
}
