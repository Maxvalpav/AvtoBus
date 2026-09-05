namespace AvtoBus.Runtime;

/// <summary>
/// Capability-маркер (аудит 03 §1.4): транспорт сам соблюдает <c>Envelope.DeliverAt</c>
/// (отложенная доставка, видимость после бэкоффа). Сейчас: InMemory, Local,
/// RabbitMQ, SQL, Azure Service Bus (scheduled). Kafka/NATS/Redis маркера нет —
/// для них задержка ретрая идёт через <see cref="IDelayedDeliveryFallback"/>.
/// </summary>
public interface ISupportsDelayedDelivery;

/// <summary>
/// Отображает физический <c>Source</c> сообщения в адрес отложенного ретрая
/// для транспортов без нативной задержки (аудит 04 §1.1). Адрес обязан быть
/// читаемым консьюмерами транспорта, иначе ретрай зависнет:
/// Kafka/Redis возвращают <c>source</c> (топик/стрим читаемы), NATS —
/// исходную подписку (сабджекты не зависят от Kind, а синтетический
/// <c>Queue("{dest}:{group}")</c> не читает никто).
/// </summary>
public interface IDelayedRetryMapper
{
    TransportDestination MapRetryDestination(
        TransportDestination source,
        TransportSubscription subscription);
}

/// <summary>
/// Единая отложенная доставка для транспортов без нативной (аудит 04 §1.1):
/// ретрай с задержкой и <c>DeferAsync</c> персистятся в <c>IScheduleStore</c>
/// и возвращаются в транспорт по наступлении срока. Реализация живёт
/// в <c>AvtoBus.Scheduling</c> и регистрируется через <c>UseScheduling</c>;
/// без неё задержка громко отбрасывается (warning), поведение — как раньше.
/// </summary>
public interface IDelayedDeliveryFallback
{
    /// <summary>
    /// Откладывает конверт на <paramref name="delay"/>. Возвращает <c>true</c> —
    /// сообщение принято в стор (исходное можно Ack), <c>false</c> — не принято
    /// (немедленный requeue как раньше).
    /// </summary>
    ValueTask<bool> TryDeferAsync(
        Envelope envelope,
        TransportDestination destination,
        string? transportName,
        TimeSpan delay,
        CancellationToken ct = default);
}
