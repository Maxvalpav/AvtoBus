namespace AvtoBus.TwoServices.Contracts;

// === События (Publish) ===
public record OrderCreated(Guid OrderId, string CustomerId, decimal Amount, DateTimeOffset CreatedAt);
public record InventoryReserved(Guid OrderId, string Sku, int Quantity);
public record InventoryFailed(Guid OrderId, string Reason);
public record OrderPaid(Guid OrderId, decimal Amount);
public record ShippingScheduled(Guid OrderId, DateTimeOffset At);

// === Команды (Send) ===
public record ReserveInventory(Guid OrderId, string Sku, int Quantity);
public record ProcessPayment(Guid OrderId, decimal Amount);

// === Запрос-Ответ ===
public record CheckStock(string Sku, int Quantity);
public record StockResult(string Sku, int Available, bool CanFulfill);
