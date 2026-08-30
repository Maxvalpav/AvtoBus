# AvtoBus: публичный C# API

## Design goals

- Minimal ceremony for simple cases.
- Full control for production cases.
- Strong typing for commands, events, queries and workflows.
- Source-generated handlers and serializers.
- No mandatory base classes for handlers.
- Effects returned explicitly from handlers.
- Same API for ASP.NET Core, Worker Service, Aspire and tests.

## Basic registration

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("db")));

builder.Services.AddAvtoBus(bus => bus
    .ApplicationName("Orders.Api")
    .AddHandlersFromAssemblyContaining<Program>()
    .UseSystemTextJson(json => json.SourceGenerated = true)
    .UseOpenTelemetry()
    .UseEfCoreDurability<AppDbContext>()
    .UseOutboxByDefault()
    .UseInboxByDefault()
    .Transports(t => t
        .UseRabbitMq("commands", rabbit => rabbit
            .ConnectionString(builder.Configuration.GetConnectionString("rabbit"))
            .AutoProvision())
        .UseKafka("events", kafka => kafka
            .BootstrapServers(builder.Configuration["Kafka:BootstrapServers"])))
    .Routing(routes => routes
        .Command<SubmitOrder>().ToRabbitQueue("orders.commands")
        .Event<OrderSubmitted>().ToKafkaTopic("orders.events.v1")
        .Event<OrderCancelled>().ToKafkaTopic("orders.events.v1"))
    .Policies(policy => policy
        .DefaultRetry(r => r.ExponentialBackoff(
            maxAttempts: 5,
            minDelay: TimeSpan.FromMilliseconds(100),
            maxDelay: TimeSpan.FromSeconds(30)))
        .DefaultDeadLetter("orders.dead")));

var app = builder.Build();

app.MapPost("/orders", async (SubmitOrder command, IAvtoBus bus, CancellationToken ct) =>
{
    var accepted = await bus.InvokeAsync<OrderAccepted>(command, ct);
    return Results.Accepted($"/orders/{accepted.OrderId}", accepted);
});

app.MapAvtoBusHealthChecks();
app.MapAvtoBusDashboard("/avtobus").RequireAuthorization("Ops");

app.Run();
```

## Message contracts

```csharp
public sealed record SubmitOrder(
    Guid OrderId,
    Guid CustomerId,
    IReadOnlyList<OrderLine> Lines) : ICommand<OrderAccepted>, IPartitionedMessage
{
    public static string SchemaName => "orders.submit-order";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record OrderAccepted(Guid OrderId, DateTimeOffset AcceptedAt);

public sealed record OrderSubmitted(
    Guid OrderId,
    Guid CustomerId,
    DateTimeOffset SubmittedAt) : IEvent, IPartitionedMessage
{
    public static string SchemaName => "orders.order-submitted";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}
```

Guidelines:

- Integration messages must be immutable records/classes.
- Do not put EF entities into messages.
- Use stable primitive DTOs.
- Always include schema version.
- Always include partition key for ordered streams.

## Pure function handler

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<(OrderAccepted Reply, OrderSubmitted Event)> Handle(
        SubmitOrder command,
        AppDbContext db,
        TimeProvider clock,
        CancellationToken ct)
    {
        if (command.Lines.Count == 0)
        {
            throw new ValidationException("Order must have at least one line.");
        }

        var now = clock.GetUtcNow();
        var order = Order.Submit(command.OrderId, command.CustomerId, command.Lines, now);

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);

        return (
            new OrderAccepted(command.OrderId, now),
            new OrderSubmitted(command.OrderId, command.CustomerId, now));
    }
}
```

The generator turns this into a pipeline:

```text
Deserialize SubmitOrder
  -> resolve AppDbContext, TimeProvider, CancellationToken
  -> run validators
  -> begin transaction and inbox if configured
  -> call SubmitOrderHandler.Handle
  -> map tuple return to reply + publish event effects
  -> persist outbox
  -> commit
```

## Compound handler: validate, load, handle

```csharp
public static class ShipOrderHandler
{
    public static ValidationResult Validate(ShipOrder command)
    {
        return command.OrderId == Guid.Empty
            ? ValidationResult.Invalid("OrderId is required.")
            : ValidationResult.Valid;
    }

    public static async ValueTask<(HandlerContinuation, Order?)> LoadAsync(
        ShipOrder command,
        AppDbContext db,
        CancellationToken ct)
    {
        var order = await db.Orders.FindAsync([command.OrderId], ct);

        return order is null
            ? (HandlerContinuation.Stop, null)
            : (HandlerContinuation.Continue, order);
    }

    public static OrderShipped Handle(ShipOrder command, Order order, TimeProvider clock)
    {
        order.MarkShipped(command.TrackingNumber, clock.GetUtcNow());
        return new OrderShipped(order.Id, command.TrackingNumber, clock.GetUtcNow());
    }
}
```

## Effects API

For complex cases, return explicit effects.

```csharp
public static class ReserveInventoryHandler
{
    public static AvtoEffects Handle(ReserveInventory command, Inventory inventory)
    {
        if (!inventory.CanReserve(command.Items))
        {
            return AvtoEffects.Publish(new InventoryReservationRejected(command.OrderId));
        }

        inventory.Reserve(command.Items);

        return AvtoEffects.All(
            AvtoEffects.Publish(new InventoryReserved(command.OrderId)),
            AvtoEffects.Schedule(
                new ReleaseInventoryReservation(command.OrderId),
                delay: TimeSpan.FromMinutes(15)),
            AvtoEffects.Reply(new ReserveInventoryAccepted(command.OrderId)));
    }
}
```

Effect types:

- Publish event.
- Send command.
- Reply to request.
- Schedule message.
- Cancel scheduled message.
- Store event in event stream.
- Start workflow.
- Signal workflow.
- Emit metric/log marker.
- Stop handler pipeline with typed problem.

## Sending and publishing

```csharp
public sealed class CheckoutService(IAvtoBus bus)
{
    public async Task<OrderAccepted> SubmitAsync(SubmitOrder command, CancellationToken ct)
    {
        return await bus.InvokeAsync<OrderAccepted>(command, ct);
    }

    public async Task PublishExternalEventAsync(OrderImported imported, CancellationToken ct)
    {
        await bus.PublishAsync(imported, ct);
    }

    public async Task ScheduleTimeoutAsync(Guid orderId, CancellationToken ct)
    {
        await bus.ScheduleAsync(
            new CancelUnpaidOrder(orderId),
            delay: TimeSpan.FromMinutes(30),
            ct);
    }
}
```

## Routing API

```csharp
bus.Routing(routes => routes
    .Command<SubmitOrder>()
        .ToRabbitQueue("orders.commands")
        .RequireSingleConsumer()
    .Event<OrderSubmitted>()
        .ToKafkaTopic("orders.events.v1")
        .PartitionBy(m => m.PartitionKey)
    .Event<OrderSubmitted>()
        .AlsoToRabbitExchange("orders.integration")
    .Query<GetOrderStatus, OrderStatus>()
        .HandleLocally()
    .Endpoint("billing")
        .ListenToRabbitQueue("billing.commands")
        .ConsumerGroup("billing-api")
        .MaxConcurrency(32));
```

## Transport-specific options

```csharp
bus.Transports(t => t
    .UseRabbitMq("rabbit", rabbit => rabbit
        .ConnectionString("amqp://guest:guest@localhost:5672")
        .UseQuorumQueues()
        .DeadLetterExchange("avto.dead")
        .DelayedRedeliveryPlugin())
    .UseKafka("kafka", kafka => kafka
        .BootstrapServers("localhost:9092")
        .DefaultProducerAcks(KafkaAcks.All)
        .EnableIdempotentProducer()
        .ConsumerGroup("orders-api")
        .UseCloudEvents())
    .UseAzureServiceBus("asb", asb => asb
        .ConnectionString("...")
        .UseSessionsFor<SubmitOrder>(m => m.PartitionKey)
        .AutoRenewLocks(maxDuration: TimeSpan.FromMinutes(10)))
    .UseNatsJetStream("nats", nats => nats
        .Url("nats://localhost:4222")
        .UsePullConsumers()
        .MaxAckPending(1024)));
```

## Policies API

```csharp
bus.Policies(policies => policies
    .ForAllMessages()
        .Timeout(TimeSpan.FromSeconds(30))
        .Retry(r => r.Immediate(3).ThenExponentialBackoff(5))
        .DeadLetterAfter(maxDeliveryAttempts: 10)
    .For<SubmitOrder>()
        .UseOutbox()
        .UseInboxDeduplication(window: TimeSpan.FromDays(7))
        .PartitionConcurrency(maxParallelism: 128)
    .ForMessagesImplementing<ISensitiveMessage>()
        .EncryptPayload()
        .MaskPayloadInLogs()
    .On<ValidationException>()
        .MoveToDeadLetter("validation")
    .On<TimeoutException>()
        .ScheduleRetry(1.Minutes(), 5.Minutes(), 15.Minutes()));
```

## Middleware API

```csharp
public static class AuditMiddleware
{
    public static void Before(AvtoEnvelope envelope, ILogger logger)
    {
        logger.LogInformation(
            "Handling {MessageType} {MessageId}",
            envelope.MessageType,
            envelope.MessageId);
    }

    public static void After(AvtoEnvelope envelope, AvtoHandlerOutcome outcome, ILogger logger)
    {
        logger.LogInformation(
            "Handled {MessageType} with {Outcome}",
            envelope.MessageType,
            outcome.Status);
    }
}

bus.Policies(policies => policies
    .ForMessagesInNamespace("Orders.Contracts")
    .AddMiddleware<AuditMiddleware>());
```

## Validators

```csharp
public sealed class SubmitOrderValidator : IAvtoValidator<SubmitOrder>
{
    public ValueTask<ValidationResult> ValidateAsync(SubmitOrder message, CancellationToken ct)
    {
        if (message.Lines.Count == 0)
        {
            return ValueTask.FromResult(ValidationResult.Invalid("Lines are required."));
        }

        return ValueTask.FromResult(ValidationResult.Valid);
    }
}
```

Validation can also integrate with FluentValidation through optional package:

```csharp
builder.Services.AddAvtoBus(bus => bus.UseFluentValidation());
```

## Sagas

```csharp
public sealed class OrderFulfillmentSaga : AvtoSaga
{
    public Guid OrderId { get; private set; }
    public bool PaymentCaptured { get; private set; }
    public bool InventoryReserved { get; private set; }

    public static Guid Correlate(OrderSubmitted message) => message.OrderId;
    public static Guid Correlate(PaymentCaptured message) => message.OrderId;
    public static Guid Correlate(InventoryReserved message) => message.OrderId;

    public AvtoEffects Start(OrderSubmitted message)
    {
        OrderId = message.OrderId;

        return AvtoEffects.All(
            AvtoEffects.Send(new CapturePayment(message.OrderId)),
            AvtoEffects.Send(new ReserveInventory(message.OrderId)),
            AvtoEffects.Schedule(new FulfillmentTimedOut(message.OrderId), TimeSpan.FromMinutes(15)));
    }

    public AvtoEffects Handle(PaymentCaptured message)
    {
        PaymentCaptured = true;
        return TryComplete();
    }

    public AvtoEffects Handle(InventoryReserved message)
    {
        InventoryReserved = true;
        return TryComplete();
    }

    private AvtoEffects TryComplete()
    {
        return PaymentCaptured && InventoryReserved
            ? AvtoEffects.All(
                AvtoEffects.Publish(new OrderReadyToShip(OrderId)),
                AvtoEffects.CompleteSaga())
            : AvtoEffects.None;
    }
}
```

## Durable workflows

```csharp
public sealed class OrderWorkflow : AvtoWorkflow<OrderWorkflowInput, OrderWorkflowResult>
{
    public override async Task<OrderWorkflowResult> RunAsync(
        OrderWorkflowInput input,
        IWorkflowContext context)
    {
        await context.ExecuteActivityAsync(
            (PaymentActivities a) => a.CaptureAsync(input.OrderId),
            new ActivityOptions { StartToCloseTimeout = TimeSpan.FromMinutes(2) });

        await context.ExecuteActivityAsync(
            (InventoryActivities a) => a.ReserveAsync(input.OrderId),
            new ActivityOptions { StartToCloseTimeout = TimeSpan.FromMinutes(2) });

        using var timeout = context.CreateTimer(TimeSpan.FromMinutes(15));

        await context.WaitSignalAsync<OrderPackedSignal>(
            signal => signal.OrderId == input.OrderId,
            timeout.Token);

        await context.ExecuteActivityAsync(
            (ShippingActivities a) => a.CreateShipmentAsync(input.OrderId),
            new ActivityOptions { StartToCloseTimeout = TimeSpan.FromMinutes(5) });

        return new OrderWorkflowResult(input.OrderId, "Completed");
    }
}
```

Workflow restrictions:

- No random, system clock, `Task.Delay` or external I/O in workflow code.
- External I/O must be in activities.
- Use `IWorkflowContext.Now`, `CreateTimer`, `ExecuteActivityAsync`, `WaitSignalAsync`.
- Analyzer `AVTO-WF001` warns about non-deterministic calls.

## Event sourcing

```csharp
public sealed class OrderAggregate : AvtoAggregate
{
    public Guid Id { get; private set; }
    public OrderStatus Status { get; private set; }

    public static OrderAggregate Create(SubmitOrder command, TimeProvider clock)
    {
        var order = new OrderAggregate();
        order.Apply(new OrderSubmitted(command.OrderId, command.CustomerId, clock.GetUtcNow()));
        return order;
    }

    public void Ship(string trackingNumber, TimeProvider clock)
    {
        if (Status != OrderStatus.Submitted)
        {
            throw new DomainException("Only submitted orders can be shipped.");
        }

        Apply(new OrderShipped(Id, trackingNumber, clock.GetUtcNow()));
    }

    private void On(OrderSubmitted e)
    {
        Id = e.OrderId;
        Status = OrderStatus.Submitted;
    }

    private void On(OrderShipped e)
    {
        Status = OrderStatus.Shipped;
    }
}
```

## Projections

```csharp
public sealed class OrderListProjection : IAvtoProjection
{
    public ValueTask HandleAsync(OrderSubmitted e, ProjectionDbContext db, CancellationToken ct)
    {
        db.OrderViews.Add(new OrderView
        {
            OrderId = e.OrderId,
            CustomerId = e.CustomerId,
            Status = "Submitted",
            UpdatedAt = e.SubmittedAt
        });

        return ValueTask.CompletedTask;
    }

    public async ValueTask HandleAsync(OrderShipped e, ProjectionDbContext db, CancellationToken ct)
    {
        var view = await db.OrderViews.FindAsync([e.OrderId], ct);
        if (view is not null)
        {
            view.Status = "Shipped";
            view.UpdatedAt = e.ShippedAt;
        }
    }
}
```

Projection options:

```csharp
bus.Projections(projections => projections
    .Add<OrderListProjection>()
    .FromStream("orders.events.v1")
    .CheckpointEvery(500)
    .Rebuildable()
    .UseInboxDeduplication());
```

## Stream processing

```csharp
bus.Streams(streams => streams
    .Topology("fraud-detection", topology => topology
        .FromKafkaTopic<PaymentCaptured>("payments.events.v1")
        .KeyBy(e => e.CustomerId.ToString("N"))
        .Window(TimeSpan.FromMinutes(5), grace: TimeSpan.FromMinutes(1))
        .Aggregate(
            seed: () => new PaymentWindow(),
            apply: (window, e) => window.Add(e))
        .Where(window => window.TotalAmount > 10_000)
        .Publish(window => new SuspiciousPaymentActivity(window.CustomerId, window.TotalAmount))));
```

## Schema registry

```csharp
bus.Schemas(schemas => schemas
    .UseAvtoRegistry(registry => registry.UsePostgreSql())
    .Compatibility(CompatibilityMode.Backward)
    .GenerateJsonSchema()
    .GenerateAsyncApi()
    .FailStartupOnBreakingChanges());
```

## Testing

```csharp
[Fact]
public async Task submit_order_publishes_order_submitted()
{
    await using var host = await AvtoBusTestHost.CreateAsync(options => options
        .AddHandlersFromAssemblyContaining<SubmitOrderHandler>()
        .UseInMemoryTransport()
        .UseInMemoryDurability());

    var accepted = await host.Bus.InvokeAsync<OrderAccepted>(new SubmitOrder(
        Guid.NewGuid(),
        Guid.NewGuid(),
        [new OrderLine("SKU-1", 2)]));

    accepted.OrderId.ShouldNotBe(Guid.Empty);

    await host.Harness.Published.ShouldContainAsync<OrderSubmitted>(e =>
        e.OrderId == accepted.OrderId);
}
```

## Analyzer rules

| Rule | Severity | Meaning |
| --- | --- | --- |
| AVTO001 | Warning | Integration message has no schema name/version |
| AVTO002 | Warning | Event routed to ordered transport has no partition key |
| AVTO003 | Error | Command has multiple owners without explicit fanout config |
| AVTO004 | Warning | Handler publishes through injected bus instead of returning effects |
| AVTO005 | Warning | Outbox disabled inside transaction-producing handler |
| AVTO006 | Error | Handler has unsupported dependency for AOT mode |
| AVTO-WF001 | Error | Workflow uses non-deterministic API |
| AVTO-WF002 | Warning | Activity has no timeout |
| AVTO-SCHEMA001 | Error | Breaking schema change detected |

## CLI examples

```bash
dotnet avto routes
dotnet avto schemas export --format asyncapi --output asyncapi.json
dotnet avto outbox stats
dotnet avto deadletter list --endpoint orders
dotnet avto deadletter replay --id 01JZ...
dotnet avto projections rebuild OrderListProjection
dotnet avto workflows list --status Running
```
