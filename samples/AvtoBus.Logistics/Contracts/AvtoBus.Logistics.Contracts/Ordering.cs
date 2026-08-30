namespace AvtoBus.Logistics.Contracts.Ordering;

/// <summary>Команда: создать заказ. Обрабатывается Orders.Service.</summary>
public record PlaceOrder(
    Guid OrderId,
    Guid CustomerId,
    IReadOnlyList<OrderLine> Lines,
    string ShippingAddress,
    Guid CorrelationId) : ICommand;

public record OrderLine(Guid Sku, int Quantity, decimal UnitPrice);

/// <summary>Событие: заказ принят в обработку.</summary>
public record OrderPlaced(Guid OrderId, Guid CustomerId, decimal Total) : IEvent;

/// <summary>Команда: отменить заказ (до отгрузки). Обрабатывается Orders.Service.</summary>
public record CancelOrder(Guid OrderId, Guid CustomerId, string Reason, Guid CorrelationId) : ICommand;

/// <summary>Событие: заказ отменён.</summary>
public record OrderCancelled(Guid OrderId, string Reason) : IEvent;

/// <summary>Команда: зарегистрировать клиента. Обрабатывается Customers.Service.</summary>
public record RegisterCustomer(Guid CustomerId, string Name, string Email, string Phone) : ICommand;

/// <summary>Событие: клиент зарегистрирован.</summary>
public record CustomerRegistered(Guid CustomerId, string Name, string Email) : IEvent;

/// <summary>Команда: добавить товар в каталог. Обрабатывается Catalog.Service.</summary>
public record CreateProduct(Guid Sku, string Name, string Category, decimal UnitPrice, int WeightGrams) : ICommand;

/// <summary>Событие: товар создан.</summary>
public record ProductCreated(Guid Sku, string Name, string Category, decimal UnitPrice) : IEvent;

/// <summary>Команда: рассчитать стоимость доставки. Обрабатывается Pricing.Service.</summary>
public record QuoteShipment(
    Guid QuoteId,
    Guid OrderId,
    string Origin,
    string Destination,
    int WeightGrams,
    string ServiceLevel) : ICommand;

/// <summary>Событие: стоимость рассчитана.</summary>
public record ShipmentQuoted(Guid QuoteId, Guid OrderId, decimal Amount, string ServiceLevel) : IEvent;