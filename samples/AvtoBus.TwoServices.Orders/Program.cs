using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Kafka;
using AvtoBus.RabbitMq;
using AvtoBus.TwoServices.Contracts;

// === Service 1: ORDERS — фичи: Publish, Send, Request/Response, Schedule, Kafka/Rabbit/InMemory ===

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddAvtoBus(bus =>
{
    // Выбор транспорта по конфигу: Kafka > Rabbit > InMemory (для демо без Docker)
    var kafka = builder.Configuration.GetConnectionString("Kafka");
    var rabbit = builder.Configuration.GetConnectionString("Rabbit");
    if (!string.IsNullOrEmpty(kafka))
        bus.UseKafka(o => { o.BootstrapServers = kafka; o.ConsumerGroup = "orders-group"; });
    else if (!string.IsNullOrEmpty(rabbit))
        bus.UseRabbitMq(o => o.ConnectionString = rabbit);
    else
        bus.UseInMemory();

    bus.Recoverability(r => r.ImmediateRetries(2).DelayedRetries(3, Backoff.Exponential(TimeSpan.FromSeconds(2))));
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName("orders-service");
});
var app = builder.Build();

app.MapGet("/", () => "Orders service — POST /orders, POST /pay/{id}, GET /stock/{sku} (Kafka/Rabbit/InMemory)");
app.MapGet("/health", () => new { status = "ok", service = "orders", transport = app.Configuration.GetConnectionString("Kafka") != null ? "kafka" : app.Configuration.GetConnectionString("Rabbit") != null ? "rabbit" : "inmemory" });

app.MapPost("/orders", async (CreateOrderRequest req, IBus bus) =>
{
    var orderId = Guid.NewGuid();
    await bus.PublishAsync(new OrderCreated(orderId, req.CustomerId, req.Amount, DateTimeOffset.UtcNow));
    await bus.SendAsync(new ReserveInventory(orderId, req.Sku, req.Quantity));
    await bus.ScheduleAsync(new ShippingScheduled(orderId, DateTimeOffset.UtcNow.AddSeconds(5)), DateTimeOffset.UtcNow.AddSeconds(5));
    return Results.AcceptedAtRoute("getOrder", new { id = orderId }, new { orderId });
});
app.MapGet("/orders/{id}", (Guid id) => new { id, status = "created" }).WithName("getOrder");

app.MapGet("/stock/{sku}", async (string sku, IBus bus, CancellationToken ct) =>
{
    try
    {
        var reply = await bus.RequestAsync<CheckStock, StockResult>(new CheckStock(sku, 1), TimeSpan.FromSeconds(3), ct);
        return Results.Ok(reply);
    }
    catch (TimeoutException) { return Results.StatusCode(504); }
});

app.MapPost("/pay/{id}", async (Guid id, IBus bus) =>
{
    await bus.PublishAsync(new OrderPaid(id, 99.9m));
    return Results.Ok(new { id, paid = true });
});

app.Run();

public record CreateOrderRequest(string CustomerId, string Sku, int Quantity, decimal Amount);

public class OrderPaidHandler : IConsumer<OrderPaid>
{
    public Task ConsumeAsync(ConsumeContext<OrderPaid> ctx)
    {
        Console.WriteLine($"[Orders] Payment confirmed for {ctx.Message.OrderId} Amount={ctx.Message.Amount}");
        return Task.CompletedTask;
    }
}
public class InventoryReservedHandler : IConsumer<InventoryReserved>
{
    public Task ConsumeAsync(ConsumeContext<InventoryReserved> ctx)
    {
        Console.WriteLine($"[Orders] Inventory reserved {ctx.Message.Sku} x{ctx.Message.Quantity} for {ctx.Message.OrderId}");
        return Task.CompletedTask;
    }
}
public class ShippingDueHandler : IConsumer<ShippingScheduled>
{
    public Task ConsumeAsync(ConsumeContext<ShippingScheduled> ctx)
    {
        Console.WriteLine($"[Orders] Shipping due for {ctx.Message.OrderId} at {ctx.Message.At}");
        return Task.CompletedTask;
    }
}
