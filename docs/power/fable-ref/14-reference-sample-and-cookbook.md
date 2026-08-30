# AvtoBus: reference production sample и cookbook

Этот документ описывает полное reference-приложение и коллекцию cookbook-рецептов, чтобы показать AvtoBus в реальных сценариях.

## Reference sample: OrderShop

Модель приложения — e-commerce order processing с полным EDA-стеком.

### Сервисы

```text
OrderShop/
├── src/
│   ├── OrderShop.Contracts/           # Shared message contracts
│   ├── OrderShop.Api/                 # ASP.NET Core Minimal API
│   ├── OrderShop.Billing.Worker/      # Payment processing
│   ├── OrderShop.Inventory.Worker/    # Inventory management
│   ├── OrderShop.Shipping.Worker/     # Shipping orchestration
│   ├── OrderShop.Notifications/       # Email/SMS notifications
│   ├── OrderShop.Analytics/           # Stream processing → ClickHouse
│   ├── OrderShop.AppHost/             # Aspire orchestration
│   └── OrderShop.ServiceDefaults/     # Shared OTel + health config
├── tests/
│   ├── OrderShop.UnitTests/
│   ├── OrderShop.ComponentTests/
│   ├── OrderShop.IntegrationTests/
│   ├── OrderShop.ContractTests/
│   ├── OrderShop.ChaosTests/
│   └── OrderShop.Benchmarks/
├── deploy/
│   ├── k8s/                           # Helm charts
│   ├── keda/                          # Autoscaling rules
│   └── grafana/                       # Dashboards
└── docs/
    ├── asyncapi.yaml                  # Generated event catalog
    └── runbooks/
```

### Contracts

```csharp
namespace OrderShop.Contracts;

// Commands
public sealed record SubmitOrder(
    Guid OrderId,
    Guid CustomerId,
    IReadOnlyList<OrderLine> Lines,
    Address ShippingAddress,
    PaymentMethod Payment) : ICommand<OrderAccepted>, IPartitionedMessage
{
    public static string SchemaName => "ordershop.orders.submit";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record CancelOrder(Guid OrderId, string Reason) : ICommand<OrderCancelled>, IPartitionedMessage
{
    public static string SchemaName => "ordershop.orders.cancel";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

// Events
public sealed record OrderSubmitted(
    Guid OrderId,
    Guid CustomerId,
    decimal TotalAmount,
    DateTimeOffset SubmittedAt) : IEvent, IPartitionedMessage
{
    public static string SchemaName => "ordershop.orders.submitted";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record PaymentCaptured(
    Guid OrderId,
    Guid PaymentId,
    decimal Amount,
    DateTimeOffset CapturedAt) : IEvent, IPartitionedMessage
{
    public static string SchemaName => "ordershop.payments.captured";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record InventoryReserved(
    Guid OrderId,
    IReadOnlyList<ReservedLine> Lines,
    DateTimeOffset ReservedAt) : IEvent, IPartitionedMessage
{
    public static string SchemaName => "ordershop.inventory.reserved";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record OrderShipped(
    Guid OrderId,
    string TrackingNumber,
    DateTimeOffset ShippedAt) : IEvent, IPartitionedMessage
{
    public static string SchemaName => "ordershop.shipping.shipped";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}
```

### OrderShop.Api Program.cs

```csharp
using OrderShop.Api;
using OrderShop.Contracts;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults(); // Aspire: OpenTelemetry, health, service discovery

builder.Services.AddDbContext<AppDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("orders-db")));

builder.Services.AddAvtoBus(bus => bus
    .ApplicationName("OrderShop.Api")
    .AddHandlersFromAssemblyContaining<Program>()
    .UseSystemTextJson()
    .UseOpenTelemetry()
    .UseEfCoreDurability<AppDbContext>()
    .UseOutboxByDefault()
    .UseInboxByDefault()
    .UseSchemaRegistry(sr => sr
        .UsePostgreSql(builder.Configuration.GetConnectionString("orders-db")!)
        .Compatibility(CompatibilityMode.Backward)
        .FailStartupOnBreakingChanges())
    .Transports(t => t
        .UseRabbitMq("commands", r => r
            .ConnectionString(builder.Configuration.GetConnectionString("rabbitmq")!)
            .UseQuorumQueues()
            .AutoProvision())
        .UseKafka("events", k => k
            .BootstrapServers(builder.Configuration["Kafka:BootstrapServers"]!)
            .UseCloudEvents()
            .EnableIdempotentProducer()))
    .Routing(r => r
        .Command<SubmitOrder>().ToRabbitQueue("ordershop.orders.commands")
        .Command<CancelOrder>().ToRabbitQueue("ordershop.orders.commands")
        .Event<OrderSubmitted>().ToKafkaTopic("ordershop.orders.events.v1")
        .Event<OrderCancelled>().ToKafkaTopic("ordershop.orders.events.v1")
        .Event<OrderShipped>().ToKafkaTopic("ordershop.orders.events.v1"))
    .Policies(p => p
        .DefaultRetry(r => r.ExponentialBackoff(
            maxAttempts: 5,
            minDelay: TimeSpan.FromMilliseconds(100),
            maxDelay: TimeSpan.FromSeconds(30)))
        .DefaultDeadLetter("ordershop.dead")
        .For<SubmitOrder>()
            .Timeout(TimeSpan.FromSeconds(10))
            .Concurrency(c => c.PartitionByMessageKey(maxParallelism: 128))
        .ForMessagesImplementing<IPii>()
            .MaskPayloadInLogs()
            .EncryptPayload(keyId: "orders-primary")));

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer();
builder.Services.AddAuthorization(o =>
{
    o.AddPolicy("Ops", p => p.RequireRole("ops"));
    o.AddPolicy("Customer", p => p.RequireAuthenticatedUser());
});

var app = builder.Build();

// Apply AvtoBus migrations at startup (dev only)
if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    await scope.ServiceProvider.GetRequiredService<IAvtoDurabilityMigrator>().MigrateAsync();
}

app.MapDefaultEndpoints();

app.MapPost("/orders", async (
    SubmitOrder command,
    IAvtoBus bus,
    ClaimsPrincipal user,
    CancellationToken ct) =>
{
    if (command.CustomerId != user.GetCustomerId())
        return Results.Forbid();

    var accepted = await bus.InvokeAsync<OrderAccepted>(command, ct);
    return Results.Accepted($"/orders/{accepted.OrderId}", accepted);
})
.RequireAuthorization("Customer")
.WithName("SubmitOrder")
.WithOpenApi();

app.MapPost("/orders/{orderId:guid}/cancel", async (
    Guid orderId, CancelOrderRequest req, IAvtoBus bus, CancellationToken ct) =>
{
    var cancelled = await bus.InvokeAsync<OrderCancelled>(
        new CancelOrder(orderId, req.Reason), ct);
    return Results.Ok(cancelled);
})
.RequireAuthorization("Customer");

app.MapGet("/orders/{orderId:guid}", async (Guid orderId, IAvtoBus bus, CancellationToken ct) =>
{
    var status = await bus.InvokeAsync<OrderStatusView>(new GetOrderStatus(orderId), ct);
    return status is null ? Results.NotFound() : Results.Ok(status);
})
.RequireAuthorization("Customer");

app.MapAvtoBusDashboard("/avtobus").RequireAuthorization("Ops");

app.Run();

public record CancelOrderRequest(string Reason);
```

### Handlers

```csharp
namespace OrderShop.Api.Handlers;

public static class SubmitOrderHandler
{
    public static ValidationResult Validate(SubmitOrder command)
    {
        if (command.Lines.Count == 0)
            return ValidationResult.Invalid("Order must have at least one line.");
        if (command.Lines.Any(l => l.Quantity <= 0))
            return ValidationResult.Invalid("All lines must have positive quantity.");
        return ValidationResult.Valid;
    }

    public static async ValueTask<(OrderAccepted Reply, OrderSubmitted Event)> Handle(
        SubmitOrder command,
        AppDbContext db,
        IPricingService pricing,
        TimeProvider clock,
        ILogger<Program> logger,
        CancellationToken ct)
    {
        logger.LogInformation("Submitting order {OrderId} for customer {CustomerId}",
            command.OrderId, command.CustomerId);

        var total = await pricing.CalculateTotalAsync(command.Lines, ct);
        var now = clock.GetUtcNow();

        var order = Order.Submit(
            command.OrderId,
            command.CustomerId,
            command.Lines,
            command.ShippingAddress,
            total,
            now);

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);

        return (
            new OrderAccepted(order.Id, now),
            new OrderSubmitted(order.Id, order.CustomerId, order.Total, now));
    }
}
```

### Saga

```csharp
namespace OrderShop.Billing.Worker;

public sealed class OrderFulfillmentSaga : AvtoSaga
{
    public Guid OrderId { get; private set; }
    public bool PaymentCaptured { get; private set; }
    public bool InventoryReserved { get; private set; }
    public string? FailureReason { get; private set; }

    public static Guid Correlate(OrderSubmitted m) => m.OrderId;
    public static Guid Correlate(PaymentCaptured m) => m.OrderId;
    public static Guid Correlate(PaymentFailed m) => m.OrderId;
    public static Guid Correlate(InventoryReserved m) => m.OrderId;
    public static Guid Correlate(InventoryUnavailable m) => m.OrderId;
    public static Guid Correlate(FulfillmentTimedOut m) => m.OrderId;

    public AvtoEffects Start(OrderSubmitted @event)
    {
        OrderId = @event.OrderId;
        return AvtoEffects.All(
            AvtoEffects.Send(new CapturePayment(OrderId, @event.TotalAmount)),
            AvtoEffects.Send(new ReserveInventory(OrderId)),
            AvtoEffects.Schedule(
                new FulfillmentTimedOut(OrderId),
                delay: TimeSpan.FromMinutes(15)));
    }

    public AvtoEffects Handle(PaymentCaptured @event)
    {
        PaymentCaptured = true;
        return TryProceed();
    }

    public AvtoEffects Handle(InventoryReserved @event)
    {
        InventoryReserved = true;
        return TryProceed();
    }

    public AvtoEffects Handle(PaymentFailed @event)
    {
        FailureReason = $"payment failed: {@event.Reason}";
        return AvtoEffects.All(
            AvtoEffects.Send(new CancelOrder(OrderId, FailureReason)),
            InventoryReserved ? AvtoEffects.Send(new ReleaseInventory(OrderId)) : AvtoEffects.None,
            AvtoEffects.CompleteSaga());
    }

    public AvtoEffects Handle(InventoryUnavailable @event)
    {
        FailureReason = $"inventory unavailable: {string.Join(",", @event.MissingSkus)}";
        return AvtoEffects.All(
            AvtoEffects.Send(new CancelOrder(OrderId, FailureReason)),
            PaymentCaptured ? AvtoEffects.Send(new RefundPayment(OrderId)) : AvtoEffects.None,
            AvtoEffects.CompleteSaga());
    }

    public AvtoEffects Handle(FulfillmentTimedOut @event)
    {
        if (PaymentCaptured && InventoryReserved)
            return AvtoEffects.None; // already completed

        FailureReason = "fulfillment timed out";
        return AvtoEffects.All(
            AvtoEffects.Send(new CancelOrder(OrderId, FailureReason)),
            PaymentCaptured ? AvtoEffects.Send(new RefundPayment(OrderId)) : AvtoEffects.None,
            InventoryReserved ? AvtoEffects.Send(new ReleaseInventory(OrderId)) : AvtoEffects.None,
            AvtoEffects.CompleteSaga());
    }

    private AvtoEffects TryProceed() =>
        PaymentCaptured && InventoryReserved
            ? AvtoEffects.All(
                AvtoEffects.Publish(new OrderReadyToShip(OrderId)),
                AvtoEffects.CompleteSaga())
            : AvtoEffects.None;
}
```

### Analytics: stream processor

```csharp
namespace OrderShop.Analytics;

public sealed class RevenueByHourProcessor
{
    public static void Configure(IAvtoStreamsBuilder streams)
    {
        streams.Topology("revenue-by-hour", topology => topology
            .FromKafkaTopic<OrderSubmitted>("ordershop.orders.events.v1")
            .KeyBy(e => e.CustomerId.ToString("N"))
            .Window(TimeSpan.FromHours(1), grace: TimeSpan.FromMinutes(5))
            .Aggregate(
                seed: () => new RevenueBucket(),
                apply: (bucket, e) => bucket.Add(e.TotalAmount))
            .Sink(bucket => new HourlyRevenueUpdated(bucket.WindowStart, bucket.TotalAmount)));
    }
}
```

### Aspire AppHost

```csharp
using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres")
    .WithPgAdmin()
    .AddDatabase("orders-db");

var rabbitmq = builder.AddRabbitMQ("rabbitmq").WithManagementPlugin();
var kafka = builder.AddKafka("kafka").WithKafkaUI();

var api = builder.AddProject<Projects.OrderShop_Api>("orders-api")
    .WithReference(postgres)
    .WithReference(rabbitmq)
    .WithReference(kafka)
    .WithReplicas(3);

var billing = builder.AddProject<Projects.OrderShop_Billing_Worker>("billing-worker")
    .WithReference(postgres)
    .WithReference(rabbitmq)
    .WithReplicas(2);

var inventory = builder.AddProject<Projects.OrderShop_Inventory_Worker>("inventory-worker")
    .WithReference(postgres)
    .WithReference(rabbitmq)
    .WithReplicas(2);

var shipping = builder.AddProject<Projects.OrderShop_Shipping_Worker>("shipping-worker")
    .WithReference(postgres)
    .WithReference(rabbitmq);

var analytics = builder.AddProject<Projects.OrderShop_Analytics>("analytics")
    .WithReference(kafka);

builder.Build().Run();
```

### Deployment

Kubernetes Deployment для api service (сокращённо):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-api
spec:
  replicas: 3
  selector: { matchLabels: { app: orders-api } }
  template:
    metadata:
      labels: { app: orders-api }
    spec:
      containers:
      - name: api
        image: registry/ordershop/orders-api:1.0.0
        env:
        - name: ConnectionStrings__orders-db
          valueFrom: { secretKeyRef: { name: orders-db, key: connection } }
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: http://otel-collector:4317
        readinessProbe:
          httpGet: { path: /health/ready, port: 8080 }
        livenessProbe:
          httpGet: { path: /health/live, port: 8080 }
        resources:
          requests: { cpu: "500m", memory: "512Mi" }
          limits: { cpu: "2", memory: "2Gi" }
```

KEDA ScaledObject для billing worker:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: billing-worker
spec:
  scaleTargetRef: { name: billing-worker }
  minReplicaCount: 2
  maxReplicaCount: 20
  triggers:
  - type: rabbitmq
    metadata:
      queueName: ordershop.billing.commands
      queueLength: "50"
      mode: QueueLength
```

## Cookbook: 20 практических рецептов

### 1. Request-response через async messaging

```csharp
public sealed record GetOrderStatus(Guid OrderId) : IQuery<OrderStatusView>;

public static class GetOrderStatusHandler
{
    public static async ValueTask<OrderStatusView?> Handle(
        GetOrderStatus query, AppDbContext db, CancellationToken ct)
    {
        var order = await db.Orders.AsNoTracking()
            .FirstOrDefaultAsync(o => o.Id == query.OrderId, ct);
        return order?.ToView();
    }
}

// В controller
var status = await bus.InvokeAsync<OrderStatusView>(new GetOrderStatus(id));
```

### 2. Fire-and-forget notification

```csharp
public sealed record SendWelcomeEmail(Guid CustomerId, string Email) : ICommand
{
    public static string SchemaName => "notifications.welcome-email";
    public static int SchemaVersion => 1;
}

// Отправка без ожидания
await bus.SendAsync(new SendWelcomeEmail(customerId, email));
```

### 3. Delayed command

```csharp
await bus.ScheduleAsync(
    new SendReminderEmail(customerId),
    delay: TimeSpan.FromDays(3));
```

### 4. Recurring background task

```csharp
public sealed class RecurringOrderCleanupService : BackgroundService
{
    private readonly IAvtoBus _bus;

    public RecurringOrderCleanupService(IAvtoBus bus) => _bus = bus;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await _bus.SendAsync(new CleanupAbandonedOrders(), ct);
            await Task.Delay(TimeSpan.FromHours(1), ct);
        }
    }
}
```

Или через Quartz.NET integration:

```csharp
services.AddAvtoBus(bus => bus.UseQuartzScheduling())
    .AddQuartzJob<CleanupAbandonedOrders>(schedule => schedule
        .Cron("0 0 * * * ?"));
```

### 5. Partitioned event processing

```csharp
bus.Routing(r => r
    .Event<OrderSubmitted>()
        .ToKafkaTopic("orders.events.v1")
        .PartitionBy(e => e.CustomerId.ToString("N")));

bus.Policies(p => p
    .For<OrderSubmitted>()
        .Concurrency(c => c.PartitionByMessageKey(maxParallelism: 64)));
```

Гарантирует, что события одного customer обрабатываются in-order.

### 6. Multi-tenant isolation

```csharp
bus.Policies(p => p
    .ForAllMessages()
        .ResolveTenantFrom(ctx => ctx.HttpContext?.User.FindFirst("tenant_id")?.Value)
        .RouteToTenantEndpoint(tenant => $"orders.{tenant}.commands"));

// Или per-tenant DB
services.AddAvtoBus(bus => bus
    .UseEfCoreDurability<AppDbContext>()
    .UsePerTenantDurability(tenant => new NpgsqlConnectionStringBuilder
    {
        Database = $"orders_{tenant}",
        Host = "postgres"
    }.ConnectionString));
```

### 7. Retry только для specific exception

```csharp
bus.Policies(p => p
    .On<HttpRequestException>()
        .Retry(r => r.ExponentialBackoff(5, TimeSpan.FromSeconds(1)))
    .On<ValidationException>()
        .MoveToDeadLetter("validation-error")
    .On<DbUpdateConcurrencyException>()
        .Retry(r => r.Immediate(3, jitter: 0.5)));
```

### 8. Circuit breaker per dependency

```csharp
bus.Policies(p => p
    .For<CapturePayment>()
        .CircuitBreaker(cb => cb
            .OpenAfter(consecutiveFailures: 10)
            .HalfOpenAfter(TimeSpan.FromSeconds(30))
            .MonitorDependency("payment-gateway")));
```

### 9. Rate limiting

```csharp
bus.Policies(p => p
    .For<SubmitOrder>()
        .RateLimit(perSecond: 1000, burst: 100)
        .PerTenantRateLimit(perSecond: 100, burst: 50));
```

### 10. Claim check для больших payloads

```csharp
bus.Policies(p => p
    .ForAllMessages()
        .UseClaimCheck(threshold: 256.Kilobytes(), store: "s3"));

// Регистрация store
services.AddAvtoBusClaimCheck("s3", opts =>
{
    opts.UseAwsS3(bucket: "ordershop-payloads", region: "us-east-1");
    opts.Retention(TimeSpan.FromDays(30));
});
```

### 11. Encryption для sensitive events

```csharp
public sealed record PaymentDetailsUpdated(Guid CustomerId, EncryptedField<PaymentInfo> Payment)
    : IEvent, IPii
{
    public static string SchemaName => "customers.payment-updated";
    public static int SchemaVersion => 1;
}

bus.Policies(p => p
    .ForMessagesImplementing<IPii>()
        .EncryptPayload(keyId: "customer-pii")
        .MaskPayloadInLogs());
```

### 12. Event upcasting

```csharp
[EventUpcaster(from: 1, to: 2)]
public static class OrderSubmittedV1ToV2 : IAvtoUpcaster<OrderSubmittedV1, OrderSubmittedV2>
{
    public static OrderSubmittedV2 Upcast(OrderSubmittedV1 old) =>
        new OrderSubmittedV2(
            OrderId: old.OrderId,
            CustomerId: old.CustomerId,
            TotalAmount: old.TotalAmount,
            Currency: "USD", // default для legacy events
            SubmittedAt: old.SubmittedAt);
}
```

### 13. Event sourcing aggregate

```csharp
public sealed class OrderAggregate : AvtoAggregate
{
    public Guid Id { get; private set; }
    public OrderStatus Status { get; private set; }
    public decimal Total { get; private set; }

    public static OrderAggregate Submit(SubmitOrder cmd, decimal total, TimeProvider clock)
    {
        var agg = new OrderAggregate();
        agg.Apply(new OrderSubmitted(cmd.OrderId, cmd.CustomerId, total, clock.GetUtcNow()));
        return agg;
    }

    public void Ship(string tracking, TimeProvider clock)
    {
        if (Status != OrderStatus.Ready)
            throw new DomainException("Order not ready to ship");
        Apply(new OrderShipped(Id, tracking, clock.GetUtcNow()));
    }

    private void On(OrderSubmitted e)
    {
        Id = e.OrderId;
        Total = e.TotalAmount;
        Status = OrderStatus.Submitted;
    }

    private void On(OrderShipped e) => Status = OrderStatus.Shipped;
}

// Usage
public static async ValueTask<OrderShipped> Handle(
    ShipOrder cmd,
    IAvtoAggregateRepository<OrderAggregate> repo,
    TimeProvider clock,
    CancellationToken ct)
{
    var agg = await repo.LoadAsync(cmd.OrderId, ct);
    agg.Ship(cmd.TrackingNumber, clock);
    await repo.SaveAsync(agg, ct);
    return new OrderShipped(agg.Id, cmd.TrackingNumber, clock.GetUtcNow());
}
```

### 14. Projection с checkpoint

```csharp
public sealed class OrderListProjection : IAvtoProjection
{
    public async ValueTask HandleAsync(
        OrderSubmitted e, ProjectionDbContext db, CancellationToken ct)
    {
        db.OrderViews.Add(new OrderView
        {
            OrderId = e.OrderId,
            CustomerId = e.CustomerId,
            Status = "Submitted",
            Total = e.TotalAmount,
            UpdatedAt = e.SubmittedAt
        });
        await db.SaveChangesAsync(ct);
    }
}

bus.Projections(p => p
    .Add<OrderListProjection>()
        .FromKafkaTopic("ordershop.orders.events.v1")
        .CheckpointEvery(500)
        .Rebuildable()
        .OnRebuild(r => r.UseShadowTable().AtomicSwap()));
```

### 15. Contract test с Pact Messages

```csharp
public class OrderSubmittedContractTests
{
    [Fact]
    public async Task order_submitted_matches_billing_contract()
    {
        var pact = new PactMessageBuilder("orders-api", "billing-worker")
            .WithInteraction("OrderSubmitted event", interaction => interaction
                .Given("an order is submitted")
                .WithContent(new OrderSubmitted(
                    OrderId: Guid.Parse("00000000-0000-0000-0000-000000000001"),
                    CustomerId: Guid.Parse("00000000-0000-0000-0000-000000000002"),
                    TotalAmount: 100m,
                    SubmittedAt: DateTimeOffset.Parse("2026-01-15T10:00:00Z"))));

        await pact.VerifyAsync();
    }
}
```

### 16. Chaos test для outbox recovery

```csharp
[Fact]
public async Task outbox_dispatches_after_broker_recovery()
{
    // Submit orders while broker up
    for (var i = 0; i < 100; i++)
        await Api.SubmitAsync(NewOrder());

    // Kill broker
    await Rabbit.StopAsync();

    // More orders → outbox grows
    for (var i = 0; i < 50; i++)
        await Api.SubmitAsync(NewOrder());

    (await Api.OutboxPendingAsync()).ShouldBeGreaterThan(50);

    // Restore
    await Rabbit.StartAsync();

    await Eventually.AssertAsync(
        async () => (await Api.OutboxPendingAsync()).ShouldBe(0),
        timeout: TimeSpan.FromMinutes(2));
}
```

### 17. Backfill new projection

```bash
# Deploy new consumer with empty projection
kubectl apply -f new-analytics-consumer.yaml

# Start backfill from event store
dotnet avto backfill run \
  --consumer revenue-analytics \
  --source kafka \
  --topic ordershop.orders.events.v1 \
  --from-timestamp 2025-01-01T00:00:00Z \
  --rate 5000 \
  --concurrency 8

# Monitor
dotnet avto backfill status --consumer revenue-analytics
```

### 18. Replay dead letters after fix

```bash
# Deploy fix
kubectl rollout status deployment/orders-api

# List dead letters caused by bug
dotnet avto deadletter list \
  --endpoint orders \
  --reason "validation" \
  --since 24h

# Bulk replay
dotnet avto deadletter replay-bulk \
  --query 'endpoint=orders AND reason=validation AND age>1h' \
  --dry-run

dotnet avto deadletter replay-bulk \
  --query 'endpoint=orders AND reason=validation AND age>1h' \
  --reason "PR #1234 fix" \
  --authorized-by ops@ordershop.com
```

### 19. Blue-green deployment с schema migration

```bash
# 1. Register new schema version (backwards compatible)
dotnet avto schemas register \
  --file schemas/order-submitted-v2.json \
  --name ordershop.orders.submitted \
  --version 2 \
  --compatibility backward

# 2. Deploy consumers that can read v1 AND v2
kubectl set image deployment/billing-worker billing=registry/billing:2.0.0

# 3. Deploy producers that emit v2
kubectl set image deployment/orders-api api=registry/api:2.0.0

# 4. Verify no v1 events in flight
dotnet avto schemas usage --name ordershop.orders.submitted --window 1h

# 5. Deprecate v1
dotnet avto schemas deprecate --name ordershop.orders.submitted --version 1
```

### 20. Zero-downtime schema evolution — add optional field

```csharp
// V1
public sealed record OrderSubmittedV1(
    Guid OrderId, Guid CustomerId, decimal TotalAmount) : IEvent
{
    public static string SchemaName => "ordershop.orders.submitted";
    public static int SchemaVersion => 1;
}

// V2 добавляет Currency как optional
public sealed record OrderSubmittedV2(
    Guid OrderId,
    Guid CustomerId,
    decimal TotalAmount,
    string Currency = "USD") : IEvent
{
    public static string SchemaName => "ordershop.orders.submitted";
    public static int SchemaVersion => 2;
}

// Upcaster
[EventUpcaster(from: 1, to: 2)]
public static class OrderSubmittedUpcaster
{
    public static OrderSubmittedV2 Upcast(OrderSubmittedV1 v1) =>
        new(v1.OrderId, v1.CustomerId, v1.TotalAmount);
}
```

Backward compatible: старые producers ещё пишут v1, новые consumers читают через upcaster в v2.

## Runbooks (в reference sample)

### Runbook 1: Outbox lag alert

```markdown
# Alert: AvtoBusOutboxLagHigh

## Impact
Downstream services не получают events в timely manner.

## Investigation
1. Check dispatcher health: `kubectl logs deployment/orders-api -c api | grep dispatcher`
2. Check DB connection: `dotnet avto health --check avtobus-outbox-orders-db`
3. Check broker: `dotnet avto health --check avtobus-transport-rabbitmq`
4. Check outbox size: `dotnet avto outbox stats`

## Common causes
- Broker down (see broker alerts)
- Dispatcher scaled to 0 (check replica count)
- DB slow (check pg_stat_statements)
- Payload too large (check `avtobus_payload_bytes` p99)

## Remediation
- Scale dispatcher: `kubectl scale deployment orders-api --replicas=5`
- Increase batch size: `AvtoBus:Outbox:BatchSize=500`
- Restart dispatcher: `kubectl rollout restart deployment orders-api`
```

### Runbook 2: Dead letter growth

```markdown
# Alert: AvtoBusDeadLetterGrowing

## Investigation
1. `dotnet avto deadletter list --endpoint orders --since 1h`
2. Group by reason: `dotnet avto deadletter stats --group-by reason`
3. Inspect top: `dotnet avto deadletter inspect --id <id>`

## Common causes
- Deploy сломал handler
- Schema breaking change
- Downstream dependency down

## Remediation
- Rollback deploy if related
- Fix code
- Replay after fix
```

## Что даёт reference sample

- Complete working example на production-realistic сценарии.
- Все layers: contracts → api → workers → analytics → deploy.
- Все test levels: unit → component → integration → contract → chaos.
- Full observability: OTel, Grafana, alerts, runbooks.
- Aspire для local dev + K8s для production.
- KEDA autoscaling.
- Schema evolution workflow.
- Failure scenarios and recovery.

Это должно быть первое, что видит разработчик после `AvtoBus.Abstractions` README.
