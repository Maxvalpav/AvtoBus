namespace AvtoBus.Observability;

/// <summary>
/// Транспорт, умеющий сообщать глубины своих очередей — источник метрики
/// <c>avtobus.queue.depth</c> (идеи 94, 302). Poll-only: счётчик читается наблюдателями.
/// </summary>
public interface IQueueDepthProvider
{
    /// <summary>Имя очереди → глубина (активные + отложенные сообщения).</summary>
    IReadOnlyDictionary<string, int> QueueDepths { get; }
}

/// <summary>
/// То же по топикам → сумма глубин очередей привязанных групп (для consumer lag метрик).
/// Необязателен: в простейшем виде хватает <see cref="IQueueDepthProvider"/>.
/// </summary>
public interface ITopicDepthProvider : IQueueDepthProvider
{
    /// <summary>Имя топика → суммарная глубина подписанных групп.</summary>
    IReadOnlyDictionary<string, int> TopicDepths { get; }
}
