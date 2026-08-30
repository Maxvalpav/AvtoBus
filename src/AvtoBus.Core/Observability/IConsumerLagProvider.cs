namespace AvtoBus.Observability;

/// <summary>
/// Источник метрики <c>avtobus.consumer.lag</c> (идея 302): отставание консьюмера —
/// сколько сообщений ещё ждёт обработки для каждой подписки. Реализуется ConsumerHost'ом,
/// считающим глубины через <see cref="IQueueDepthProvider"/> / <see cref="ITopicDepthProvider"/>.
/// </summary>
public interface IConsumerLagProvider
{
    /// <summary>Имя destination (очередь или топик) → число необработанных сообщений.</summary>
    IReadOnlyDictionary<string, long> ConsumerLags { get; }
}
