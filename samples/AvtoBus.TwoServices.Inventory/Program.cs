using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Kafka;
using AvtoBus.RabbitMq;
using AvtoBus.TwoServices.Contracts;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddAvtoBus(bus =>
{
    var kafka = builder.Configuration.GetConnectionString("Kafka");
    var rabbit = builder.Configuration.GetConnectionString("Rabbit");
    if (!string.IsNullOrEmpty(kafka))
        bus.UseKafka(o => { o.BootstrapServers = kafka; o.ConsumerGroup = "inventory-group"; o.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest; });
    else if (!string.IsNullOrEmpty(rabbit))
        bus.UseRabbitMq(o => o.ConnectionString = rabbit);
    else
        bus.UseInMemory();

    bus.Recoverability(r => r.ImmediateRetries(2).DelayedRetries(2, Backoff.Linear(TimeSpan.FromSeconds(1))));
    bus.Routes(r => r.Command<CheckStock>().ToQueue("check-stock"));
    bus.UseInboxDeduplication(TimeSpan.FromHours(1));
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName("inventory-service");
});
var app = builder.Build();

app.MapGet("/", () => "Inventory service — Kafka/Rabbit/InMemory, consumes OrderCreated/ReserveInventory, replies CheckStock");
app.MapGet("/health", () => new { status = "ok", service = "inventory", transport = !string.IsNullOrEmpty(app.Configuration.GetConnectionString("Kafka")) ? "kafka" : !string.IsNullOrEmpty(app.Configuration.GetConnectionString("Rabbit")) ? "rabbit" : "inmemory" });
app.Run();

public class OrderCreatedHandler : IConsumer<OrderCreated>
{
    private readonly ILogger<OrderCreatedHandler> _log;
    public OrderCreatedHandler(ILogger<OrderCreatedHandler> log) => _log = log;
    public Task ConsumeAsync(ConsumeContext<OrderCreated> ctx)
    {
        _log.LogInformation("[Inventory] OrderCreated {OrderId} Customer={CustomerId} Amount={Amount}", ctx.Message.OrderId, ctx.Message.CustomerId, ctx.Message.Amount);
        return Task.CompletedTask;
    }
}

public class ReserveInventoryHandler : IConsumer<ReserveInventory>
{
    private readonly ILogger<ReserveInventoryHandler> _log;
    public ReserveInventoryHandler(ILogger<ReserveInventoryHandler> log) => _log = log;
    public async Task ConsumeAsync(ConsumeContext<ReserveInventory> ctx)
    {
        _log.LogInformation("[Inventory] Reserving {Sku} x{Quantity} for {OrderId} (via {Transport})", ctx.Message.Sku, ctx.Message.Quantity, ctx.Message.OrderId, ctx.Envelope.Headers.ContainsKey("kafka-topic") ? "kafka" : "other");
        if (ctx.Message.Quantity > 100) throw new InvalidOperationException("Too many items");
        await ctx.PublishAsync(new InventoryReserved(ctx.Message.OrderId, ctx.Message.Sku, ctx.Message.Quantity));
    }
}

public class CheckStockHandler : IConsumer<CheckStock>
{
    public async Task ConsumeAsync(ConsumeContext<CheckStock> ctx)
    {
        var available = 42;
        var result = new StockResult(ctx.Message.Sku, available, ctx.Message.Quantity <= available);
        await ctx.RespondAsync(result);
    }
}

public class ShippingHandler : IConsumer<ShippingScheduled>
{
    private readonly ILogger<ShippingHandler> _log;
    public ShippingHandler(ILogger<ShippingHandler> log) => _log = log;
    public Task ConsumeAsync(ConsumeContext<ShippingScheduled> ctx)
    {
        _log.LogInformation("[Inventory] Shipping ready for {OrderId}", ctx.Message.OrderId);
        return Task.CompletedTask;
    }
}
