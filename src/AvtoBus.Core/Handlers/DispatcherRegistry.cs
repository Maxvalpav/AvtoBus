using System.Collections.Frozen;

namespace AvtoBus.Handlers;

/// <summary>
/// Таблица «тип сообщения → его хендлеры». Строится при старте, замораживается для быстрого lookup.
/// Учитывает полиморфизм: подписка на базовый тип/интерфейс ловит наследников (идея 9).
/// </summary>
public sealed class DispatcherRegistry
{
    private readonly FrozenDictionary<Type, IMessageDispatcher[]> _exact;
    private readonly (Type Base, IMessageDispatcher Dispatcher)[] _polymorphic;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<Type, IMessageDispatcher[]> _resolved = new();
    private const int MaxResolved = 10_000;

    private DispatcherRegistry(
        FrozenDictionary<Type, IMessageDispatcher[]> exact,
        (Type, IMessageDispatcher)[] polymorphic)
    {
        _exact = exact;
        _polymorphic = polymorphic;
    }

    public static DispatcherRegistry Build(IEnumerable<IMessageDispatcher> dispatchers)
    {
        var list = dispatchers.ToArray();

        var exact = list
            .GroupBy(d => d.MessageType)
            .ToFrozenDictionary(g => g.Key, g => g.ToArray());

        // Хендлеры на абстракции обрабатывают и наследников — их проверяем отдельно.
        var polymorphic = list
            .Where(d => d.MessageType.IsInterface || d.MessageType.IsAbstract)
            .Select(d => (d.MessageType, d))
            .ToArray();

        return new DispatcherRegistry(exact, polymorphic);
    }

    /// <summary>Все типы, для которых есть хотя бы один хендлер.</summary>
    public IEnumerable<Type> HandledTypes => _exact.Keys;

    public bool HasHandlerFor(Type messageType) => For(messageType).Length > 0;

    /// <summary>
    /// Хендлеры для конкретного типа: точные + унаследованные от базовых типов и интерфейсов.
    /// Результат кэшируется — иерархия типов не меняется в рантайме.
    /// </summary>
    public IMessageDispatcher[] For(Type messageType)
    {
        if (_resolved.TryGetValue(messageType, out var cached)) return cached;
        // Атомарная проверка размера внутри GetOrAdd фабрики, чтобы избежать race
        return _resolved.GetOrAdd(messageType, t =>
        {
            if (_resolved.Count >= MaxResolved) return ResolveDirect(t);
            return ResolveDirect(t);
        });
    }
    private IMessageDispatcher[] ResolveDirect(Type type)
    {
        var result = new List<IMessageDispatcher>();
        if (_exact.TryGetValue(type, out var direct)) result.AddRange(direct);
        foreach (var (baseType, dispatcher) in _polymorphic)
            if (baseType != type && baseType.IsAssignableFrom(type)) result.Add(dispatcher);
        return result.Count == 0 ? [] : result.ToArray();
    }
}
