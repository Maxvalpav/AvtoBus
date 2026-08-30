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

        while (_chain.TryGetValue((eventType, version), out var upcaster))
        {
            current = upcaster.Upcast(current);
            version = upcaster.ToVersion;
        }

        return current;
    }
}
