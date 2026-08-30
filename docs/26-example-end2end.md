# 🛒 Полный пример: e-commerce на AvtoBus

> **Illustrative scenario / not runnable yet.** Это связный пример целевого API, а не существующий sample solution.

Три сервиса + общая библиотека контрактов:
- `Orders` (API, принимает команды, владеет OrderSaga)
- `Payments` (обрабатывает оплаты, публикует `PaymentSucceeded/Failed`)
- `Shipping` (создаёт отгрузки)
- `Contracts` (общий NuGet со всеми сообщениями)

## 1. Решение и структура

```
/ecommerce
├── Contracts/
│   ├── OrderCommands.cs
│   ├── OrderEvents.cs
│   ├── PaymentCommands.cs
│   └── PaymentEvents.cs
├── Orders/
│   ├── Program.cs
│   ├── OrderSaga.cs
│   └── Controllers/OrdersController.cs
├── Payments/
│   ├── Program.cs
│   └── ChargeHandler.cs
├── Shipping/
│   ├── Program.cs
│   └── CreateShipmentHandler.cs
└── docker-compose.yml
```

## 2. Контракты (`Contracts/`)

```csharp
// Contracts/OrderCommands.cs
namespace Contracts;

public sealed record PlaceOrder(
    Guid OrderId,
    string CustomerId,
    OrderItem[] Items,
    decimal Total
) : ICommand;

public sealed record CancelOrder(Guid OrderId, string Reason) : ICommand;

public sealed record OrderItem(string Sku, int Qty, decimal Price);
```

```csharp
// Contracts/OrderEvents.cs
namespace Contracts;

public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total) : IEvent;
public sealed record OrderPaid(Guid OrderId, Guid PaymentId) : IEvent;
public sealed record OrderShipped(Guid OrderId, Guid ShipmentId) : IEvent;
public sealed record OrderFulfilled(Guid OrderId) : IEvent;
public sealed record OrderCancelled(Guid OrderId, string Reason) : IEvent;
```

```csharp
// Contracts/PaymentCommands.cs
namespace Contracts;
public sealed record ChargeCard(Guid OrderId, string CardToken, decimal Amount) : ICommand;
```

```csharp
// Contracts/PaymentEvents.cs
namespace Contracts;
public sealed record PaymentSucceeded(Guid PaymentId, Guid OrderId, decimal Amount) : IEvent;
public sealed record PaymentFailed(Guid PaymentId, Guid OrderId, string Reason) : IEvent;
```

## 3. Payments Service

```csharp
// Payments/Program.cs
using AvtoBus;
using Payments;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAvtoBus(b => b
    .UseRabbitMq(builder.Configuration.GetConnectionString("Rabbit")!)
    .AddConsumersFromAssembly(typeof(Program).Assembly)
    .UseInboxDeduplication()
    .Recoverability(r => r
        .ImmediateRetries(2)
        .DelayedRetries(3, Backoff.Exponential(TimeSpan.FromSeconds(3)))
        .MapException<PaymentDeclinedException>(FailureAction.Discard)));

var app = builder.Build();
app.MapAvtoBusDashboard("/bus");
app.Run();
```

```csharp
// Payments/ChargeHandler.cs
namespace Payments;

public static class ChargeHandler
{
    public static async Task<PaymentSucceeded> Handle(
        ChargeCard cmd,
        IPaymentGateway gateway,
        ILogger<ChargeHandler> log,
        CancellationToken ct)
    {
        log.LogInformation("Charging card for order {OrderId}", cmd.OrderId);
        var result = await gateway.Charge(cmd.CardToken, cmd.Amount, ct);

        return result.IsSuccess
            ? new PaymentSucceeded(result.PaymentId, cmd.OrderId, cmd.Amount)
            : throw new PaymentDeclinedException(cmd.OrderId, result.Error!);
    }
}

// PaymentDeclinedException → не ретраится, но сага заказывает компенсацию (отмену заказа)
public sealed class PaymentDeclinedException : Exception
{
    public Guid OrderId { get; }
    public PaymentDeclinedException(Guid orderId, string reason) : base(reason) => OrderId = orderId;
}
```

## 4. Shipping Service

```csharp
// Shipping/Program.cs
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddAvtoBus(b => b
    .UseRabbitMq(builder.Configuration.GetConnectionString("Rabbit")!)
    .AddConsumersFromAssembly(typeof(Program).Assembly));
var app = builder.Build();
app.MapAvtoBusDashboard("/bus");
app.Run();
```

```csharp
// Shipping/CreateShipmentHandler.cs
namespace Shipping;

public static class ShipmentHandler
{
    public static async Task<OrderShipped> Handle(
        OrderPaid evt,
        IShipmentService shipments,
        CancellationToken ct)
    {
        var shipment = await shipments.Create(evt.OrderId, ct);
        return new OrderShipped(evt.OrderId, shipment.Id);
    }
}
```

## 5. Orders Service (сага + API)

```csharp
// Orders/Program.cs
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrdersDb>(o => o.UseNpgsql(cs));
builder.Services.AddAvtoBus(b => b
    .UseRabbitMq(builder.Configuration.GetConnectionString("Rabbit")!)
    .UseOutbox<OrdersDb>()
    .AddConsumersFromAssembly(typeof(Program).Assembly)
    .AddSaga<OrderSaga, OrderSagaState>(opts =>
    {
        opts.Sla(from: typeof(OrderPlaced), to: typeof(OrderFulfilled), max: TimeSpan.FromHours(2));
    })
    .Recoverability(r => r.ImmediateRetries(3).DelayedRetries(5, Backoff.DecorrelatedJitter(5.Seconds())))
    .Pipeline(p =>
    {
        p.UseOpenTelemetry();
        p.UseFluentValidation();
    }));

builder.Services.AddAvtoBusDashboard();

var app = builder.Build();
app.MapAvtoBusDashboard("/bus");
app.MapOrdersApi();
await app.DatabaseMigrateAsync();
app.Run();
```

```csharp
// Orders/OrderSaga.cs
namespace Orders;

public sealed class OrderSagaState : SagaState
{
    public Guid OrderId { get; set; }
    public decimal Total { get; set; }
    public string? CardToken { get; set; }
    public Guid? PaymentId { get; set; }
    public Guid? ShipmentId { get; set; }
    public bool Paid { get; set; }
    public bool Shipped { get; set; }
}

public sealed class OrderSaga : Saga<OrderSagaState>,
    IStartedBy<PlaceOrder>,
    IHandle<PaymentSucceeded>,
    IHandle<PaymentFailed>,
    IHandle<OrderShipped>,
    IHandle<CancelOrder>
{
    protected override void Correlate(SagaMap<OrderSagaState> map)
    {
        map.On<PlaceOrder>(m => m.OrderId).StartsNew();
        map.On<PaymentSucceeded>(m => m.OrderId);
        map.On<PaymentFailed>(m => m.OrderId);
        map.On<OrderShipped>(m => m.OrderId);
        map.On<CancelOrder>(m => m.OrderId);
    }

    public Task Handle(PlaceOrder m)
    {
        State.OrderId = m.OrderId;
        State.Total = m.Total;
        State.CardToken = "fake-token-" + m.CustomerId;
        State.Status = "AwaitingPayment";

        // Отправляем команду на оплату
        return Send(new ChargeCard(m.OrderId, State.CardToken, m.Total)).AsTask();
    }

    public Task Handle(PaymentSucceeded m)
    {
        State.Paid = true;
        State.PaymentId = m.PaymentId;
        State.Status = "Shipping";

        return Send(new RequestShipment(m.OrderId)).AsTask();
    }

    public Task Handle(PaymentFailed m)
    {
        State.Status = "Cancelled";
        MarkComplete();
        return Publish(new OrderCancelled(State.OrderId, "payment-declined")).AsTask();
    }

    public Task Handle(OrderShipped m)
    {
        State.Shipped = true;
        State.ShipmentId = m.ShipmentId;
        State.Status = "Fulfilled";
        MarkComplete();
        return Publish(new OrderFulfilled(State.OrderId)).AsTask();
    }

    public Task Handle(CancelOrder m)
    {
        if (State.Paid)
        {
            // Уже оплачен — запускаем возврат
            return Send(new Refund(State.PaymentId!.Value, State.Total)).AsTask();
        }
        MarkComplete();
        State.Status = "Cancelled";
        return Publish(new OrderCancelled(State.OrderId, m.Reason)).AsTask();
    }
}
```

```csharp
// Orders/Controllers/OrdersController.cs
namespace Orders.Controllers;

public static class OrdersApi
{
    public static void MapOrdersApi(this WebApplication app)
    {
        app.MapPost("/api/orders", async (
            PlaceOrderRequest req, IBus bus, CancellationToken ct) =>
        {
            var orderId = Guid.CreateVersion7();
            var cmd = new PlaceOrder(orderId, req.CustomerId, req.Items,
                req.Items.Sum(i => i.Price * i.Qty));
            await bus.Send(cmd, cancellationToken: ct);
            return Results.Accepted($"/api/orders/{orderId}", new { orderId });
        })
        .WithName("PlaceOrder");

        app.MapGet("/api/orders/{id:guid}", async (
            Guid id, OrdersDb db, CancellationToken ct) =>
        {
            var order = await db.OrderViews.FindAsync([id], ct);
            return order is null ? Results.NotFound() : Results.Ok(order);
        });
    }
}

public sealed record PlaceOrderRequest(string CustomerId, OrderItemDto[] Items);
public sealed record OrderItemDto(string Sku, int Qty, decimal Price);
```

## 6. `docker-compose.yml` (всё вместе)

```yaml
services:
  rabbit:
    image: rabbitmq:4-management-alpine
    ports: ["5672:5672", "15672:15672"]
    environment:
      RABBITMQ_QUORUM_GROUP_SIZE: 1   # for dev single-node
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s

  postgres:
    image: postgres:17-alpine
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app, POSTGRES_DB: orders }
    ports: ["5432:5432"]
    volumes: ["pg:/var/lib/postgresql/data"]

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["4317:4317", "16686:16686"]

  orders:
    build: ./Orders
    depends_on:
      rabbit: { condition: service_healthy }
      postgres: { condition: service_started }
    environment:
      ConnectionStrings__Rabbit: amqp://guest:guest@rabbit:5672
      ConnectionStrings__Db: Host=postgres;Database=orders;Username=app;Password=app
    ports: ["5000:8080"]

  payments:
    build: ./Payments
    depends_on:
      rabbit: { condition: service_healthy }
    environment:
      ConnectionStrings__Rabbit: amqp://guest:guest@rabbit:5672
    ports: ["5001:8080"]

  shipping:
    build: ./Shipping
    depends_on:
      rabbit: { condition: service_healthy }
    environment:
      ConnectionStrings__Rabbit: amqp://guest:guest@rabbit:5672
    ports: ["5002:8080"]

volumes: { pg: {} }
```

## 7. Запуск и проверка

```bash
docker compose up -d --build

# Создаём заказ
curl -X POST http://localhost:5000/api/orders -H 'Content-Type: application/json' -d '{
  "customerId": "cust-42",
  "items": [
    { "sku": "sku-001", "qty": 2, "price": 1500 }
  ]
}'

# Наблюдаем в дашборде: http://localhost:5000/bus
# Трейсы в Jaeger: http://localhost:16686
```

**Поток в Jaeger:**
```
POST /api/orders → PlaceOrder (Orders Handler)
  ├── Outbox enqueue (Postgres)
  └── Outbox relay sends ChargeCard to RabbitMQ
        └── ChargeHandler (Payments) → PaymentSucceeded
              ├── Outbox enqueue
              └── relay → PaymentSucceeded fan-out
                    ├── OrderSaga (Orders): state→Shipping, sends RequestShipment
                    └── any other future subscribers (notifications, analytics, ...)
                          └── ShipmentHandler (Shipping) → OrderShipped
                                └── OrderSaga: MarkComplete, publish OrderFulfilled
```

## 8. Тест потока целиком

```csharp
// Tests/EndToEnd.cs
[Collection("compose")]
public class EndToEnd
{
    [Fact]
    public async Task Full_fulfillment_flow()
    {
        await using var h = await AvtoBusTestHarness.CreateAsync(s =>
        {
            s.AddSaga<OrderSaga, OrderSagaState>();
            s.AddConsumer<ChargeHandler>();
            s.AddConsumer<ShipmentHandler>();
        });

        var orderId = Guid.NewGuid();
        await h.Bus.Send(new PlaceOrder(orderId, "cust-42",
            [new OrderItem("sku-1", 2, 1500)], 3000));

        await h.WaitForPublished<OrderFulfilled>(TimeSpan.FromSeconds(30));

        var saga = await h.Sagas.For<OrderSagaState>(s => s.OrderId == orderId);
        saga.Status.Should().Be("Fulfilled");
    }

    [Fact]
    public async Task Payment_failure_cancels_order()
    {
        await using var h = await AvtoBusTestHarness.CreateAsync(s =>
        {
            s.AddSaga<OrderSaga, OrderSagaState>();
            s.AddSingleton<IPaymentGateway>(new FailingGateway());
            s.AddConsumer<ChargeHandler>();
        });

        var id = Guid.NewGuid();
        await h.Bus.Send(new PlaceOrder(id, "cust-42", Array.Empty<OrderItem>(), 100));
        await h.AdvanceTime(TimeSpan.FromMinutes(5)); // все ретраи + DLQ
        await h.Transport.DrainAsync();

        h.Published<OrderCancelled>().Should().ContainSingle(c => c.OrderId == id);
    }
}
```

## 9. Что этот пример демонстрирует

- ✅ **Команды** отправляются по именованным очередям, ровно один получатель
- ✅ **События** fan-out ко всем подписчикам
- ✅ **Сага** с корреляцией, SLA-монитором и компенсацией при отмене
- ✅ **Outbox** гарантирует отсутствие «принял HTTP, забыл послать в брокер»
- ✅ **Inbox** не даёт повторной обработки при ретраях брокера
- ✅ **Recoverability** — transient ретраятся, business — сразу в DLQ
- ✅ **Observability** — OTel-трейсы в Jaeger из коробки, дашборд на всех сервисах
- ✅ **Тест-харнесс** — полный прогон саги без инфраструктуры, за миллисекунды
