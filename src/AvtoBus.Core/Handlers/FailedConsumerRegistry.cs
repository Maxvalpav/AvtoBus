using System.Collections.Frozen;

namespace AvtoBus.Handlers;

/// <summary>
/// Таблица «тип сообщения → обработчик второй линии обороны» (идея 169).
/// Поиск по точному типу: вторая линия привязана к конкретному контракту.
/// </summary>
public sealed class FailedConsumerRegistry(IEnumerable<IFailedConsumerDispatcher> dispatchers)
{
    private readonly FrozenDictionary<Type, IFailedConsumerDispatcher> _byType =
        dispatchers.ToFrozenDictionary(d => d.MessageType);

    public IFailedConsumerDispatcher? For(Type messageType)
        => _byType.TryGetValue(messageType, out var dispatcher) ? dispatcher : null;

    public bool IsEmpty => _byType.Count == 0;
}
