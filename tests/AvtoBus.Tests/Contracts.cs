namespace AvtoBus.Tests.Contracts;

// Контракты тестов живут в отдельном namespace: конвенция именования берёт из него префикс,
// поэтому имена на проводе получаются вида "contracts.place-order".

public sealed record PlaceOrder(Guid OrderId, string CustomerId, decimal Total) : ICommand;

public sealed record OrderPlaced(Guid OrderId, decimal Total) : IEvent;

public sealed record OrderPaid(Guid OrderId) : IEvent;

public sealed record ReceiptRequested(Guid OrderId, string TxId) : IEvent;

public sealed record ShipmentCreated(Guid OrderId) : IEvent;

public sealed record ChargeCard(Guid OrderId, decimal Amount) : ICommand;

public sealed record CancelOrder(Guid OrderId) : ICommand;

// Тип только для ScopeLifetimeTests: scope на попытку подтверждается
// сравнением InstanceId scoped-сервиса в middleware и хендлере.
public sealed record ScopedCommand(Guid Id) : ICommand;

public sealed record SendReminder(Guid OrderId) : ICommand;

// Локальные in-process очереди (идея 15): тип используется только local-тестами,
// чтобы не смешивать маршрутизацию с остальными контрактами.
public sealed record LocalJob(Guid Id, string Payload) : ICommand;

[MessageAlias("orders.legacy-renamed.v2", "orders.legacy-renamed.v1")]
public sealed record RenamedContract(string Value) : IEvent;

public sealed record GetQuote(string Symbol) : ICommand;

public sealed record QuoteResult(string Symbol, decimal Price);

public sealed record AccountEvent(string AccountId, int Sequence) : IEvent
{
    [PartitionKey]
    public string Key => AccountId;
}

// Партиционированная обработка (идея 25) через receive-side селектор OrderedBy:
// у контракта НЕТ [PartitionKey], ключ достаётся десериализацией в роутере.
public sealed record PartitionedJob(Guid Id, string Key, int Sequence) : ICommand;

public interface IOrderEvent : IEvent
{
    Guid OrderId { get; }
}

public sealed record OrderArchived(Guid OrderId) : IOrderEvent;

[Message(Ttl = "00:00:00.001")]
public sealed record ShortLived(string Payload) : IEvent;

// Уникальные типы для тестов, слушающих глобальные ActivityListener/MeterListener:
// параллельные харнессы пишут в те же источники, и общий тип размазывает их события (см. TraceDecisionTests).
public sealed record TraceTrackedEvent(Guid Id) : IEvent;

public sealed record TimeoutProbe(Guid Id) : IEvent;

// Сквозной трейс publish → consume: тип используется ровно одним тестом,
// чтобы ActivityListener не смешивал спаны параллельных харнессов.
public sealed record TraceFlowEvent(Guid Id) : IEvent;
