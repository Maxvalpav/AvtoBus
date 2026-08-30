# AvtoBus: side-by-side migration cookbook

Реальные примеры кода для миграции с популярных .NET messaging библиотек. Каждый раздел — self-contained и показывает before/after.

## Из MediatR

### Handler

**MediatR:**

```csharp
public record SubmitOrderCommand(Guid OrderId, Guid CustomerId) : IRequest<OrderAccepted>;

public class SubmitOrderHandler : IRequestHandler<SubmitOrderCommand, OrderAccepted>
{
    private readonly AppDbContext _db;
    private readonly ILogger<SubmitOrderHandler> _logger;

    public SubmitOrderHandler(AppDbContext db, ILogger<SubmitOrderHandler> logger)
    {
        _db = db;
        _logger = logger;
    }

    public async Task<OrderAccepted> Handle(SubmitOrderCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Submitting {OrderId}", request.OrderId);
        var order = new Order(request.OrderId, request.CustomerId);
        _db.Orders.Add(order);
        await _db.SaveChangesAsync(cancellationToken);
        return new OrderAccepted(request.OrderId);
    }
}
```

**AvtoBus:**

```csharp
public sealed record SubmitOrder(Guid OrderId, Guid CustomerId) : ICommand<OrderAccepted>
{
    public static string SchemaName => "orders.submit-order";
    public static int SchemaVersion => 1;
}

public static class SubmitOrderHandler
{
    public static async ValueTask<OrderAccepted> Handle(
        SubmitOrder command,
        AppDbContext db,
        ILogger<Program> logger,
        CancellationToken ct)
    {
        logger.LogInformation("Submitting {OrderId}", command.OrderId);
        var order = new Order(command.OrderId, command.CustomerId);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        return new OrderAccepted(command.OrderId);
    }
}
```

Изменения:

- `IRequest<T>` → `ICommand<T>` (marker interface, но с schema metadata).
- Класс handler → static class + static method (pure function).
- Constructor injection → method parameter injection.
- `Handle(request, ct)` → `Handle(command, dependencies, ct)`.

### Notification / event

**MediatR:**

```csharp
public record OrderSubmittedNotification(Guid OrderId) : INotification;

public class SendConfirmationEmailHandler : INotificationHandler<OrderSubmittedNotification>
{
    public Task Handle(OrderSubmittedNotification notification, CancellationToken cancellationToken)
    {
        // send email
        return Task.CompletedTask;
    }
}
```

**AvtoBus:**

```csharp
public sealed record OrderSubmitted(Guid OrderId) : IEvent
{
    public static string SchemaName => "orders.order-submitted";
    public static int SchemaVersion => 1;
}

public static class SendConfirmationEmailHandler
{
    public static ValueTask Handle(OrderSubmitted @event, IEmailService email, CancellationToken ct)
        => email.SendConfirmationAsync(@event.OrderId, ct);
}
```

### Publishing

**MediatR:**

```csharp
await _mediator.Send(new SubmitOrderCommand(...));
await _mediator.Publish(new OrderSubmittedNotification(...));
```

**AvtoBus:**

```csharp
await bus.InvokeAsync<OrderAccepted>(new SubmitOrder(...));
await bus.PublishAsync(new OrderSubmitted(...));
```

Или лучше — не публиковать вручную, а возвращать из handler как cascading effect:

```csharp
public static async ValueTask<(OrderAccepted Reply, OrderSubmitted Event)> Handle(
    SubmitOrder command, AppDbContext db, CancellationToken ct)
{
    var order = new Order(command.OrderId, command.CustomerId);
    db.Orders.Add(order);
    await db.SaveChangesAsync(ct);
    return (new OrderAccepted(order.Id), new OrderSubmitted(order.Id));
}
```

### Pipeline behavior

**MediatR:**

```csharp
public class LoggingBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, ...)
    {
        _logger.LogInformation("Handling {Name}", typeof(TRequest).Name);
        var response = await next();
        _logger.LogInformation("Handled {Name}", typeof(TRequest).Name);
        return response;
    }
}

// Registration
services.AddTransient(typeof(IPipelineBehavior<,>), typeof(LoggingBehavior<,>));
```

**AvtoBus:**

```csharp
public static class LoggingMiddleware
{
    public static void Before(AvtoEnvelope envelope, ILogger<Program> logger)
        => logger.LogInformation("Handling {Type}", envelope.MessageType);

    public static void After(AvtoEnvelope envelope, ILogger<Program> logger)
        => logger.LogInformation("Handled {Type}", envelope.MessageType);
}

// Registration
bus.Policies(p => p.ForAllMessages().AddMiddleware<LoggingMiddleware>());
```

### Registration

**MediatR:**

```csharp
services.AddMediatR(cfg => cfg.RegisterServicesFromAssemblyContaining<Program>());
```

**AvtoBus (drop-in local mode):**

```csharp
services.AddAvtoBus(bus => bus
    .ApplicationName("Orders")
    .AddHandlersFromAssemblyContaining<Program>()
    .UseInMemoryTransport()
    .UseInMemoryDurability());
```

Плюс для integration event'ов — outbox и transport автоматически используются, как только настроены.

### MediatR shim

Для gradual migration можно использовать shim из `AvtoBus.Shims.MediatR`:

```csharp
// Меняем только using
using Mediator = MediatR;  // старое
using Mediator = AvtoBus.Shims.MediatR;  // новое

public class OldHandler : IRequestHandler<OldRequest, OldResponse>
{
    // код не меняется
}
```

## Из MassTransit

### Consumer

**MassTransit:**

```csharp
public class SubmitOrderConsumer : IConsumer<SubmitOrder>
{
    private readonly AppDbContext _db;

    public SubmitOrderConsumer(AppDbContext db) => _db = db;

    public async Task Consume(ConsumeContext<SubmitOrder> context)
    {
        var order = new Order(context.Message.OrderId);
        _db.Orders.Add(order);
        await _db.SaveChangesAsync(context.CancellationToken);
        await context.Publish(new OrderSubmitted(order.Id));
    }
}
```

**AvtoBus:**

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<OrderSubmitted> Handle(
        SubmitOrder command,
        AppDbContext db,
        CancellationToken ct)
    {
        var order = new Order(command.OrderId);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        return new OrderSubmitted(order.Id);
    }
}
```

### Saga state machine

**MassTransit:**

```csharp
public class OrderFulfillmentSagaState : SagaStateMachineInstance
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = default!;
    public bool PaymentCaptured { get; set; }
    public bool InventoryReserved { get; set; }
}

public class OrderFulfillmentStateMachine : MassTransitStateMachine<OrderFulfillmentSagaState>
{
    public State Awaiting { get; } = null!;
    public State Completed { get; } = null!;

    public Event<OrderSubmitted> OrderSubmitted { get; } = null!;
    public Event<PaymentCaptured> PaymentCaptured { get; } = null!;
    public Event<InventoryReserved> InventoryReserved { get; } = null!;

    public OrderFulfillmentStateMachine()
    {
        InstanceState(x => x.CurrentState);

        Event(() => OrderSubmitted, x => x.CorrelateById(m => m.Message.OrderId));
        Event(() => PaymentCaptured, x => x.CorrelateById(m => m.Message.OrderId));
        Event(() => InventoryReserved, x => x.CorrelateById(m => m.Message.OrderId));

        Initially(
            When(OrderSubmitted)
                .Then(ctx => { /* ... */ })
                .Publish(ctx => new CapturePayment(ctx.Message.OrderId))
                .Publish(ctx => new ReserveInventory(ctx.Message.OrderId))
                .TransitionTo(Awaiting));

        During(Awaiting,
            When(PaymentCaptured).Then(ctx => ctx.Saga.PaymentCaptured = true),
            When(InventoryReserved).Then(ctx => ctx.Saga.InventoryReserved = true));
    }
}
```

**AvtoBus:**

```csharp
public sealed class OrderFulfillmentSaga : AvtoSaga
{
    public Guid OrderId { get; private set; }
    public bool PaymentCaptured { get; private set; }
    public bool InventoryReserved { get; private set; }

    public static Guid Correlate(OrderSubmitted m) => m.OrderId;
    public static Guid Correlate(PaymentCaptured m) => m.OrderId;
    public static Guid Correlate(InventoryReserved m) => m.OrderId;

    public AvtoEffects Start(OrderSubmitted @event)
    {
        OrderId = @event.OrderId;
        return AvtoEffects.All(
            AvtoEffects.Send(new CapturePayment(@event.OrderId)),
            AvtoEffects.Send(new ReserveInventory(@event.OrderId)));
    }

    public AvtoEffects Handle(PaymentCaptured @event)
    {
        PaymentCaptured = true;
        return TryComplete();
    }

    public AvtoEffects Handle(InventoryReserved @event)
    {
        InventoryReserved = true;
        return TryComplete();
    }

    private AvtoEffects TryComplete() =>
        PaymentCaptured && InventoryReserved
            ? AvtoEffects.All(
                AvtoEffects.Publish(new OrderReadyToShip(OrderId)),
                AvtoEffects.CompleteSaga())
            : AvtoEffects.None;
}
```

Разница:

- Нет explicit state machine DSL — состояние выражено полями класса.
- Correlation через generated static methods, не через fluent config.
- Effects через return values, не через `Publish`/`Send` в контексте.

### Registration

**MassTransit:**

```csharp
services.AddMassTransit(x =>
{
    x.AddConsumer<SubmitOrderConsumer>();
    x.AddSagaStateMachine<OrderFulfillmentStateMachine, OrderFulfillmentSagaState>()
        .EntityFrameworkRepository(r =>
        {
            r.ConcurrencyMode = ConcurrencyMode.Optimistic;
            r.ExistingDbContext<AppDbContext>();
        });

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("localhost", "/");
        cfg.ConfigureEndpoints(context);
    });
});
```

**AvtoBus:**

```csharp
services.AddAvtoBus(bus => bus
    .ApplicationName("Orders")
    .AddHandlersFromAssemblyContaining<Program>()
    .UseEfCoreDurability<AppDbContext>()
    .UseOutboxByDefault()
    .UseInboxByDefault()
    .Transports(t => t.UseRabbitMq("main", r => r.ConnectionString("amqp://localhost")))
    .Routing(routes => routes
        .Command<SubmitOrder>().ToRabbitQueue("orders.commands")
        .Event<OrderSubmitted>().ToRabbitExchange("orders.events")));
```

### Interop mode

Для gradual migration, AvtoBus может exchange messages с MassTransit endpoints:

```csharp
bus.Transports(t => t.UseRabbitMq("main", r => r
    .ConnectionString("amqp://localhost")
    .UseMassTransitInterop(mt => mt
        .MapMessageAssembly(typeof(SharedContracts.SubmitOrder).Assembly))));
```

Wire format: AvtoBus читает MassTransit envelope headers (`MT-*`), сохраняет correlation IDs.

## Из NServiceBus

### Handler

**NServiceBus:**

```csharp
public class SubmitOrderHandler : IHandleMessages<SubmitOrder>
{
    public async Task Handle(SubmitOrder message, IMessageHandlerContext context)
    {
        var db = context.SynchronizedStorageSession.GetService<AppDbContext>();
        var order = new Order(message.OrderId);
        db.Orders.Add(order);
        await db.SaveChangesAsync();
        await context.Publish(new OrderSubmitted(order.Id));
    }
}
```

**AvtoBus:**

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<OrderSubmitted> Handle(
        SubmitOrder command,
        AppDbContext db,
        CancellationToken ct)
    {
        var order = new Order(command.OrderId);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        return new OrderSubmitted(order.Id);
    }
}
```

### Recoverability

**NServiceBus:**

```csharp
var recoverability = endpointConfiguration.Recoverability();
recoverability.Immediate(i => i.NumberOfRetries(3));
recoverability.Delayed(d => d.NumberOfRetries(2).TimeIncrease(TimeSpan.FromSeconds(15)));
```

**AvtoBus:**

```csharp
bus.Policies(p => p
    .ForAllMessages()
        .Retry(r => r
            .Immediate(3)
            .Then().Delayed(2, initialDelay: TimeSpan.FromSeconds(15), factor: 2)));
```

### Saga

**NServiceBus:**

```csharp
public class OrderSagaData : ContainSagaData
{
    public virtual Guid OrderId { get; set; }
    public virtual bool PaymentCaptured { get; set; }
}

public class OrderSaga : Saga<OrderSagaData>,
    IAmStartedByMessages<OrderSubmitted>,
    IHandleMessages<PaymentCaptured>
{
    protected override void ConfigureHowToFindSaga(SagaPropertyMapper<OrderSagaData> mapper)
    {
        mapper.ConfigureMapping<OrderSubmitted>(m => m.OrderId).ToSaga(s => s.OrderId);
        mapper.ConfigureMapping<PaymentCaptured>(m => m.OrderId).ToSaga(s => s.OrderId);
    }

    public Task Handle(OrderSubmitted message, IMessageHandlerContext context)
    {
        Data.OrderId = message.OrderId;
        return context.Send(new CapturePayment(message.OrderId));
    }

    public Task Handle(PaymentCaptured message, IMessageHandlerContext context)
    {
        Data.PaymentCaptured = true;
        MarkAsComplete();
        return context.Publish(new OrderReadyToShip(Data.OrderId));
    }
}
```

**AvtoBus:** см. пример выше (уже упрощён vs MassTransit).

## Из CAP

### Publish

**CAP:**

```csharp
public class OrderService
{
    private readonly ICapPublisher _cap;
    private readonly AppDbContext _db;

    public async Task SubmitAsync(Guid orderId, Guid customerId)
    {
        using var trans = _db.Database.BeginTransaction(_cap, autoCommit: true);
        var order = new Order(orderId, customerId);
        _db.Orders.Add(order);
        await _db.SaveChangesAsync();
        await _cap.PublishAsync("orders.submitted", new OrderSubmitted(orderId));
    }
}
```

**AvtoBus:**

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<(OrderAccepted, OrderSubmitted)> Handle(
        SubmitOrder command, AppDbContext db, CancellationToken ct)
    {
        var order = new Order(command.OrderId, command.CustomerId);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        return (new OrderAccepted(order.Id), new OrderSubmitted(order.Id));
    }
}
```

Outbox используется автоматически, без явного `BeginTransaction(cap)`. Транзакция handler и outbox insert коммитятся вместе.

### Subscribe

**CAP:**

```csharp
public class OrderSubscriber : ICapSubscribe
{
    [CapSubscribe("orders.submitted")]
    public Task Handle(OrderSubmitted @event) { /* ... */ }
}
```

**AvtoBus:**

```csharp
public static class OrderSubmittedHandler
{
    public static ValueTask Handle(OrderSubmitted @event, IEmailService email, CancellationToken ct)
        => email.SendConfirmationAsync(@event.OrderId, ct);
}
```

Routing связывает event с destination:

```csharp
bus.Routing(r => r.Event<OrderSubmitted>().ToRabbitQueue("orders.submitted"));
```

### Registration

**CAP:**

```csharp
services.AddCap(x =>
{
    x.UseEntityFramework<AppDbContext>();
    x.UseRabbitMQ("localhost");
    x.UseDashboard();
});
```

**AvtoBus:**

```csharp
services.AddAvtoBus(bus => bus
    .UseEfCoreDurability<AppDbContext>()
    .UseOutboxByDefault()
    .Transports(t => t.UseRabbitMq("main", r => r.ConnectionString("amqp://localhost")))
    .Routing(...));

app.MapAvtoBusDashboard("/avtobus").RequireAuthorization("Ops");
```

## Из Wolverine

Wolverine ближе всех по API к AvtoBus, поэтому миграция минимальна.

**Wolverine:**

```csharp
public static class SubmitOrderHandler
{
    public static async Task<OrderSubmitted> Handle(SubmitOrder command, IDocumentSession session)
    {
        session.Store(new Order(command.OrderId));
        return new OrderSubmitted(command.OrderId);
    }
}

builder.Host.UseWolverine(opts =>
{
    opts.UseRabbitMq(...);
    opts.PublishMessage<OrderSubmitted>().ToRabbitExchange("events");
});
```

**AvtoBus:**

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<OrderSubmitted> Handle(
        SubmitOrder command, IDocumentSession session, CancellationToken ct)
    {
        session.Store(new Order(command.OrderId));
        return new OrderSubmitted(command.OrderId);
    }
}

builder.Services.AddAvtoBus(bus => bus
    .Transports(t => t.UseRabbitMq(...))
    .Routing(r => r.Event<OrderSubmitted>().ToRabbitExchange("events")));
```

Разница:

- `Task` → `ValueTask` (perf preference).
- Explicit `CancellationToken` требуется в AvtoBus.
- Registration через `IServiceCollection` (Aspire-friendly) вместо `UseWolverine` extension.

## Из Dapr

### Publish

**Dapr:**

```csharp
public class OrdersController : ControllerBase
{
    private readonly DaprClient _dapr;

    public OrdersController(DaprClient dapr) => _dapr = dapr;

    [HttpPost]
    public async Task<IActionResult> Submit(SubmitOrder command)
    {
        await SaveToDbAsync(command);
        await _dapr.PublishEventAsync("pubsub", "orders", new OrderSubmitted(command.OrderId));
        return Accepted();
    }
}
```

**AvtoBus:**

```csharp
app.MapPost("/orders", async (SubmitOrder command, IAvtoBus bus, CancellationToken ct) =>
{
    var accepted = await bus.InvokeAsync<OrderAccepted>(command, ct);
    return Results.Accepted($"/orders/{accepted.OrderId}", accepted);
});
```

### Subscribe

**Dapr:**

```csharp
[Topic("pubsub", "orders")]
[HttpPost("/orders/submitted")]
public IActionResult HandleOrderSubmitted(OrderSubmitted @event)
{
    // handle
    return Ok();
}
```

**AvtoBus:**

```csharp
public static class OrderSubmittedHandler
{
    public static ValueTask Handle(OrderSubmitted @event, IEmailService email, CancellationToken ct)
        => email.SendConfirmationAsync(@event.OrderId, ct);
}
```

### Hybrid: AvtoBus как domain framework + Dapr как transport

AvtoBus может использовать Dapr Pub/Sub component как transport:

```csharp
bus.Transports(t => t.UseDapr("pubsub-1", d => d
    .PubSubComponentName("pubsub")
    .SidecarEndpoint("http://localhost:3500")
    .UseCloudEvents()));
```

Это даёт лучшее из двух миров: Dapr для infrastructure portability + AvtoBus для C# domain ergonomics.

## Общая migration strategy

### Phase 1 — coexistence (weeks)

1. Установить AvtoBus рядом со старым framework.
2. Настроить interop mode (для MassTransit/NServiceBus) или общий broker.
3. Начать писать новые handler'ы в AvtoBus style.
4. Общий broker обеспечивает cross-framework messaging.

### Phase 2 — handler migration (weeks-months)

1. Мигрировать handlers группами по bounded context.
2. Заменять `IRequest`/`IConsumer`/`IHandleMessages` → pure functions.
3. Move from context.Publish/Send → cascading effects.
4. Обеспечивать backward compatibility через shim adapters.

### Phase 3 — infrastructure migration (weeks)

1. Мигрировать sagas → AvtoBus sagas.
2. Мигрировать outbox → AvtoBus durability.
3. Мигрировать dashboards → AvtoBus dashboard.
4. Мигрировать recoverability policies.

### Phase 4 — cleanup (weeks)

1. Удалить старый framework packages.
2. Удалить interop adapters.
3. Обновить monitoring/alerting на AvtoBus metrics.
4. Обновить runbooks и docs.

### Rollback plan

- Каждая phase должна быть reversible через feature flag.
- Interop adapters дают возможность откатить handlers обратно на старый framework.
- Outbox schema AvtoBus совместима с чтением extenal tool (CAP-подобные схемы можно прочитать).

## Что нельзя мигрировать один-в-один

- **MassTransit routing slips** → AvtoBus workflow (переписать).
- **NServiceBus data bus / attachments** → AvtoBus claim check pattern.
- **CAP versioning через attributes** → AvtoBus schema registry.
- **Wolverine's Marten integration** → AvtoBus event sourcing package.
- **Dapr state store** → AvtoBus использует own durability stores.
- **Dapr distributed lock** → используйте PostgreSQL advisory locks или Redis directly.

## Verification checklist после миграции

- [ ] Все integration events имеют schema name/version.
- [ ] Outbox работает под load (chaos test).
- [ ] Inbox работает под redelivery (chaos test).
- [ ] Sagas корректно retry при concurrent messages.
- [ ] Dashboard authorization настроен.
- [ ] Metrics/alerts переключены на AvtoBus namespace.
- [ ] AsyncAPI export работает и включён в CI.
- [ ] Contract tests с consumers pass.
- [ ] Perf regression tests показывают no degradation.
- [ ] Runbooks обновлены под AvtoBus terminology.
