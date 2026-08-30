namespace AvtoBus;

/// <summary>
/// Куда отправляется сообщение. Транспорт сам решает, что это: очередь, exchange, topic или subject.
/// </summary>
/// <param name="Name">Имя назначения, например <c>place-order</c> или <c>orders.order-placed</c>.</param>
/// <param name="Kind">Очередь (один владелец) или топик (fan-out).</param>
public readonly record struct TransportDestination(string Name, DestinationKind Kind)
{
    public static TransportDestination Queue(string name) => new(name, DestinationKind.Queue);

    public static TransportDestination Topic(string name) => new(name, DestinationKind.Topic);

    public override string ToString() => $"{Kind.ToString().ToLowerInvariant()}:{Name}";
}

public enum DestinationKind
{
    Queue,
    Topic,
}

/// <summary>Что именно слушает консьюмер.</summary>
/// <param name="Destination">Очередь или топик.</param>
/// <param name="ConsumerGroup">
/// Группа консьюмеров: подписчики одной группы делят сообщения топика, разных — получают копии каждый.
/// </param>
/// <param name="PrefetchCount">Сколько сообщений транспорт может отдать до подтверждения.</param>
public readonly record struct TransportSubscription(
    TransportDestination Destination,
    string ConsumerGroup,
    int PrefetchCount = 32);

/// <summary>
/// Сообщение, полученное из транспорта. Подтверждение — обязанность вызывающего:
/// без <see cref="AcknowledgeAsync"/> или <see cref="RejectAsync"/> сообщение вернётся после lease-таймаута.
/// </summary>
public interface ITransportMessage
{
    Envelope Envelope { get; }

    /// <summary>
    /// Фактический источник, из которого сообщение было вычитано. Для топика это физическая
    /// очередь группы консьюмеров, а не имя топика: DLQ/retry наследуют её имя (идея 164).
    /// </summary>
    TransportDestination Source { get; }

    /// <summary>Обработка успешна: сообщение можно удалить.</summary>
    ValueTask AcknowledgeAsync(CancellationToken ct = default);

    /// <summary>Обработка провалена. <paramref name="requeue"/> — вернуть в очередь или отправить в DLQ.</summary>
    ValueTask RejectAsync(bool requeue, CancellationToken ct = default);
}

/// <summary>
/// Минимальный контракт транспорта: отправить и получать (идея 51).
/// Новый транспорт пишется за день — всё остальное делает ядро.
/// </summary>
public interface ITransport : IAsyncDisposable
{
    /// <summary>Имя транспорта для мульти-транспортной маршрутизации: <c>rabbitmq</c>, <c>kafka</c>, <c>inmemory</c>.</summary>
    string Name { get; }

    ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default);

    IAsyncEnumerable<ITransportMessage> ReceiveAsync(TransportSubscription subscription, CancellationToken ct = default);

    /// <summary>
    /// Идемпотентно создаёт топологию: очереди, топики, привязки, DLQ (идея 55).
    /// Вызывается один раз при старте, до подъёма консьюмеров.
    /// </summary>
    ValueTask ProvisionAsync(IReadOnlyCollection<TransportDestination> destinations, CancellationToken ct = default);
}
