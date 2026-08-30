# AvtoBus: полный testing guide

Тестирование EDA-систем сложнее, чем обычных request/response API. AvtoBus предоставляет multi-level testing story: от unit-тестов handler'ов до contract, chaos и golden envelope тестов.

## Пирамида тестов для EDA

```text
                    ┌──────────────────┐
                    │ Chaos / Recovery │       (rare, expensive)
                    └──────────────────┘
                  ┌──────────────────────┐
                  │ End-to-end multi-svc │
                  └──────────────────────┘
                ┌──────────────────────────┐
                │ Contract (Pact / async)  │
                └──────────────────────────┘
              ┌──────────────────────────────┐
              │ Integration (Testcontainers) │
              └──────────────────────────────┘
            ┌────────────────────────────────────┐
            │ Component (in-process AvtoBusTestHost) │
            └────────────────────────────────────┘
          ┌──────────────────────────────────────────┐
          │  Unit (pure function handlers, no infra) │       (many, fast)
          └──────────────────────────────────────────┘
```

## Level 1: Unit tests

Pure function handlers тестируются без AvtoBus вообще.

```csharp
public class SubmitOrderHandlerTests
{
    [Fact]
    public async Task submitting_order_returns_accepted_and_publishes_submitted()
    {
        // Arrange
        var db = TestDbContext.InMemory();
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-01-15T10:00:00Z"));
        var command = new SubmitOrder(
            OrderId: Guid.NewGuid(),
            CustomerId: Guid.NewGuid(),
            Lines: [new OrderLine("SKU-1", 2)]);

        // Act
        var result = await SubmitOrderHandler.Handle(command, db, clock, CancellationToken.None);

        // Assert
        result.Reply.OrderId.ShouldBe(command.OrderId);
        result.Reply.AcceptedAt.ShouldBe(clock.GetUtcNow());
        result.Event.OrderId.ShouldBe(command.OrderId);

        var saved = await db.Orders.FindAsync(command.OrderId);
        saved.ShouldNotBeNull();
    }

    [Fact]
    public async Task submitting_order_without_lines_throws_validation()
    {
        var db = TestDbContext.InMemory();
        var clock = FakeTimeProvider.System;
        var command = new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(), Lines: []);

        var act = () => SubmitOrderHandler.Handle(command, db, clock, CancellationToken.None);

        await act.ShouldThrowAsync<ValidationException>();
    }
}
```

Хорошие практики:

- Нет mocks: prefer real in-memory alternatives.
- Fake time via `TimeProvider` (стандарт .NET 8+).
- Assert по value, не по behavior of mocks.

## Level 2: Component tests с AvtoBusTestHost

Тестируется полный AvtoBus pipeline с in-memory transport и in-memory store.

```csharp
public class SubmitOrderComponentTests : IAsyncLifetime
{
    private AvtoBusTestHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AvtoBusTestHost.CreateAsync(options => options
            .AddHandlersFromAssemblyContaining<SubmitOrderHandler>()
            .UseInMemoryTransport()
            .UseInMemoryDurability()
            .UseSystemTextJson()
            .OverrideTimeProvider(new FakeTimeProvider(DateTimeOffset.Parse("2026-01-15T10:00:00Z"))));
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task invoke_publishes_event_via_outbox_and_transport()
    {
        var command = new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("SKU-1", 1)]);

        var accepted = await _host.Bus.InvokeAsync<OrderAccepted>(command);

        // Verify outbox
        var outboxRows = await _host.Durability.OutboxAsync();
        outboxRows.ShouldContain(r => r.MessageType == typeof(OrderSubmitted).FullName);

        // Verify transport received message (after dispatcher pass)
        await _host.DispatcherTickAsync();

        _host.Harness.Published.ShouldContain<OrderSubmitted>(e => e.OrderId == accepted.OrderId);
        _host.Harness.Sent.ShouldBeEmpty();
        _host.Harness.DeadLettered.ShouldBeEmpty();
    }

    [Fact]
    public async Task handler_failure_moves_to_dead_letter_after_retries()
    {
        _host.Chaos.OnHandler<SubmitOrder>(_ => throw new InvalidOperationException("boom"));

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await _host.Bus.InvokeAsync<OrderAccepted>(
                new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("SKU-1", 1)])));

        _host.Harness.DeadLettered.ShouldNotBeEmpty();
        var dl = _host.Harness.DeadLettered.First();
        dl.Reason.ShouldContain("InvalidOperationException");
        dl.AttemptCount.ShouldBe(_host.Options.MaxAttempts);
    }
}
```

### AvtoBusTestHarness API

```csharp
public interface IAvtoBusTestHarness
{
    IReadOnlyList<PublishedRecord> Published { get; }
    IReadOnlyList<SentRecord> Sent { get; }
    IReadOnlyList<ScheduledRecord> Scheduled { get; }
    IReadOnlyList<DeadLetterRecord> DeadLettered { get; }
    IReadOnlyList<InboxRecord> InboxRecorded { get; }

    ValueTask WaitForPublishedAsync<T>(TimeSpan timeout, Predicate<T>? filter = null);
    ValueTask WaitForSagaStateAsync<T>(string sagaId, Predicate<T> state, TimeSpan timeout);
    ValueTask WaitForWorkflowCompletedAsync(string workflowId, TimeSpan timeout);

    void Clear();
}
```

### Deterministic time для sagas и workflows

```csharp
[Fact]
public async Task saga_timeout_fires_after_15_minutes()
{
    var orderId = Guid.NewGuid();
    var time = new FakeTimeProvider(DateTimeOffset.Parse("2026-01-15T10:00:00Z"));
    _host.OverrideTimeProvider(time);

    await _host.Bus.PublishAsync(new OrderSubmitted(orderId, Guid.NewGuid(), time.GetUtcNow()));

    _host.Harness.Scheduled.ShouldContain<FulfillmentTimedOut>(m => m.OrderId == orderId);

    // Advance time; scheduled message fires
    time.Advance(TimeSpan.FromMinutes(16));
    await _host.SchedulerTickAsync();

    await _host.Harness.WaitForPublishedAsync<FulfillmentTimedOut>(timeout: TimeSpan.FromSeconds(5));
}
```

### Workflow time-travel

Workflows тестируются через deterministic replay:

```csharp
[Fact]
public async Task order_workflow_completes_after_payment_and_inventory()
{
    var input = new OrderWorkflowInput(Guid.NewGuid());
    var runner = _host.WorkflowRunner<OrderWorkflow, OrderWorkflowInput, OrderWorkflowResult>();

    // Start
    var handle = await runner.StartAsync(input);

    // Simulate activity results
    await runner.CompleteActivityAsync<PaymentActivities>(
        a => a.CaptureAsync(input.OrderId), returnValue: null);

    await runner.CompleteActivityAsync<InventoryActivities>(
        a => a.ReserveAsync(input.OrderId), returnValue: null);

    // Send signal
    await handle.SignalAsync(new OrderPackedSignal(input.OrderId));

    // Advance timer (if timer was set)
    await runner.AdvanceTimeAsync(TimeSpan.FromMinutes(1));

    await runner.CompleteActivityAsync<ShippingActivities>(
        a => a.CreateShipmentAsync(input.OrderId), returnValue: null);

    var result = await handle.GetResultAsync(TimeSpan.FromSeconds(5));
    result.Status.ShouldBe("Completed");

    // Verify history is deterministic on replay
    await runner.ReplayFromHistoryAsync(handle.WorkflowId);
}
```

## Level 3: Integration tests с Testcontainers

Настоящий broker и настоящая БД, но локально через containers.

```csharp
[Collection("integration")]
public class SubmitOrderIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer _postgres = null!;
    private RabbitMqContainer _rabbit = null!;
    private WebApplicationFactory<Program> _app = null!;

    public async Task InitializeAsync()
    {
        _postgres = new PostgreSqlBuilder()
            .WithImage("postgres:17")
            .Build();
        _rabbit = new RabbitMqBuilder()
            .WithImage("rabbitmq:4-management")
            .Build();

        await Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync());

        _app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, cfg) =>
                {
                    cfg.AddInMemoryCollection([
                        new("ConnectionStrings:db", _postgres.GetConnectionString()),
                        new("ConnectionStrings:rabbit", _rabbit.GetConnectionString()),
                    ]);
                });
            });

        // Apply AvtoBus migrations
        using var scope = _app.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IAvtoDurabilityMigrator>().MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _app.DisposeAsync();
        await _postgres.DisposeAsync();
        await _rabbit.DisposeAsync();
    }

    [Fact]
    public async Task submit_order_end_to_end_via_http_and_broker()
    {
        var client = _app.CreateClient();
        var command = new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("SKU-1", 1)]);

        var response = await client.PostAsJsonAsync("/orders", command);

        response.StatusCode.ShouldBe(HttpStatusCode.Accepted);
        var accepted = await response.Content.ReadFromJsonAsync<OrderAccepted>();
        accepted!.OrderId.ShouldBe(command.OrderId);

        // Wait for OrderSubmitted event to reach downstream consumer
        var billingReceived = await WaitForRabbitMessageAsync<OrderSubmitted>(
            queue: "billing.orders",
            timeout: TimeSpan.FromSeconds(10));

        billingReceived.OrderId.ShouldBe(command.OrderId);
    }
}
```

## Level 4: Contract tests

### Pact Message

Producer описывает schemas и примеры сообщений, consumer verifies contract.

```csharp
public class OrderSubmittedContractTests
{
    [Fact]
    public async Task order_submitted_matches_billing_service_contract()
    {
        var pact = new PactBuilder("orders-api", "billing-service")
            .WithMessageInteraction("order_submitted event", interaction => interaction
                .Given("an order is submitted")
                .WithMetadata("content-type", "application/json")
                .WithContent(new OrderSubmitted(
                    OrderId: Guid.NewGuid(),
                    CustomerId: Guid.NewGuid(),
                    SubmittedAt: DateTimeOffset.UtcNow))
                .WithSchema(schema => schema
                    .Field("OrderId", MatchType.Uuid)
                    .Field("CustomerId", MatchType.Uuid)
                    .Field("SubmittedAt", MatchType.Iso8601)));

        await pact.PublishAsync();
    }
}
```

### AsyncAPI contract

CI job:

```bash
dotnet avto schemas export --format asyncapi --output asyncapi-current.yaml
asyncapi diff asyncapi-baseline.yaml asyncapi-current.yaml --fail-on breaking
```

## Level 5: End-to-end multi-service

Docker Compose с полным stack (Postgres, RabbitMQ, Kafka, orders-api, billing-worker, inventory-worker):

```yaml
services:
  postgres: { image: postgres:17 }
  rabbitmq: { image: rabbitmq:4-management }
  kafka: { image: confluentinc/cp-kafka:7.7 }
  orders-api:
    build: ./src/Orders.Api
    depends_on: [postgres, rabbitmq, kafka]
  billing-worker:
    build: ./src/Billing.Worker
    depends_on: [postgres, rabbitmq]
  inventory-worker:
    build: ./src/Inventory.Worker
    depends_on: [postgres, rabbitmq]
```

Тесты через HTTP:

```csharp
[Fact]
public async Task order_flows_from_api_through_billing_and_inventory()
{
    // Submit
    var command = new SubmitOrder(...);
    var accepted = await OrdersApi.SubmitAsync(command);

    // Wait for saga to complete
    await Eventually.AssertAsync(async () =>
    {
        var status = await OrdersApi.GetStatusAsync(command.OrderId);
        status.ShouldBe("ReadyToShip");
    }, timeout: TimeSpan.FromSeconds(30));
}
```

## Level 6: Chaos and recovery tests

Через `AvtoChaos` API или chaos-mesh.

```csharp
[Fact]
public async Task outbox_dispatches_after_broker_recovery()
{
    // Given broker is up, submit orders
    for (var i = 0; i < 100; i++)
        await OrdersApi.SubmitAsync(NewOrder());

    // Kill broker
    await _rabbit.StopAsync();

    // Submit more orders — they go to outbox
    for (var i = 0; i < 50; i++)
        await OrdersApi.SubmitAsync(NewOrder());

    // Outbox growth
    var lag = await OrdersApi.GetMetricAsync("avtobus_outbox_pending");
    lag.ShouldBeGreaterThan(50);

    // Recover broker
    await _rabbit.StartAsync();

    // Wait for dispatcher to catch up
    await Eventually.AssertAsync(async () =>
    {
        var pending = await OrdersApi.GetMetricAsync("avtobus_outbox_pending");
        pending.ShouldBe(0);
    }, timeout: TimeSpan.FromMinutes(2));

    // No dead letters (transient failures)
    var deadLetters = await OrdersApi.GetMetricAsync("avtobus_dead_letter_total");
    deadLetters.ShouldBe(0);
}
```

## Golden envelope tests

Проверка, что envelope wire format стабилен между версиями.

```csharp
public class EnvelopeSerializationGoldenTests
{
    [Theory]
    [MemberData(nameof(GoldenEnvelopes))]
    public void envelope_serializes_to_expected_bytes(string filename, AvtoEnvelope envelope)
    {
        var serialized = AvtoBusJson.Serialize(envelope);
        var expected = File.ReadAllText($"golden/{filename}");

        serialized.ShouldBe(expected);
    }

    public static IEnumerable<object[]> GoldenEnvelopes() =>
    [
        ["order-submitted-v1.json", new AvtoEnvelope { /* fixed values */ }],
        ["payment-captured-v2.json", new AvtoEnvelope { /* fixed values */ }],
    ];
}
```

Golden files коммитятся в репозиторий. При изменении serialization — тест падает, требуя явного обновления golden файлов и bumping AvtoBus version.

## Property-based tests

Для complex handler логики полезен FsCheck / CsCheck:

```csharp
[Property]
public Property idempotent_handler_produces_same_effect_for_duplicates(SubmitOrder command)
{
    var db = TestDbContext.InMemory();
    var clock = FakeTimeProvider.System;

    var first = SubmitOrderHandler.Handle(command, db, clock, default).Result;
    var second = SubmitOrderHandler.Handle(command, db, clock, default).Result;

    return (first.Reply.OrderId == second.Reply.OrderId).ToProperty();
}
```

## Mutation testing

Stryker.NET для проверки, что тесты действительно проверяют логику:

```bash
dotnet stryker --project Orders.Api --threshold-high 80 --threshold-low 60
```

Целевые уровни для AvtoBus проектов:

- Handlers: 90%+ mutation score.
- Sagas: 85%+ mutation score.
- Routing/policies: 70%+.

## Performance regression tests

BenchmarkDotNet:

```csharp
[MemoryDiagnoser]
public class SubmitOrderBenchmark
{
    private AvtoBusTestHost _host = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _host = await AvtoBusTestHost.CreateAsync(o => o
            .AddHandlersFromAssemblyContaining<SubmitOrderHandler>()
            .UseInMemoryTransport()
            .UseInMemoryDurability());
    }

    [Benchmark]
    public async Task<OrderAccepted> Handle_Local() =>
        await _host.Bus.InvokeAsync<OrderAccepted>(
            new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("SKU-1", 1)]));
}
```

CI проверяет, что regression не более X%:

```bash
dotnet run -c Release --project Orders.Bench -- --filter "*SubmitOrder*"
```

Compare с baseline через `dotnet-benchmark-compare`.

## Test data и fixtures

`AvtoBus.Testing.Fixtures` package:

```csharp
public static class OrderFixtures
{
    public static SubmitOrder ValidCommand(Action<SubmitOrderBuilder>? configure = null)
    {
        var builder = new SubmitOrderBuilder()
            .WithOrderId(Guid.NewGuid())
            .WithCustomerId(Guid.NewGuid())
            .WithLine("SKU-1", 1);

        configure?.Invoke(builder);
        return builder.Build();
    }
}
```

## Test naming convention

Рекомендуется snake_case для теста и business language:

- `submitting_order_with_zero_lines_returns_validation_error`
- `payment_captured_completes_saga_after_inventory_reserved`
- `workflow_survives_process_crash_and_resumes_from_history`

## Testing checklist

Для каждого handler:

- [ ] Happy path unit test.
- [ ] Sad path unit tests (validation, missing resources).
- [ ] Component test через `AvtoBusTestHost`.
- [ ] Golden envelope test для integration events.
- [ ] Contract test если event пересекает service boundary.

Для каждой saga:

- [ ] Start message → correct commands emitted.
- [ ] Follow-up messages → state transitions correct.
- [ ] Timeout → correct compensation.
- [ ] Concurrent messages → optimistic concurrency retry.

Для каждого workflow:

- [ ] Happy path deterministic execution.
- [ ] Activity failure → retry.
- [ ] Signal handling.
- [ ] Query returns current state.
- [ ] Replay from history produces same result.
- [ ] Non-deterministic API triggers analyzer error.

Для каждого transport binding:

- [ ] Integration test через Testcontainers.
- [ ] Trace propagation тест.
- [ ] Failure и recovery тест.

## Anti-patterns в тестах

- **Assert через логи** — используй domain events и store snapshots.
- **`Thread.Sleep(...)` в тестах** — используй `Eventually.AssertAsync` с timeout.
- **Shared state между тестами** — используй `IAsyncLifetime` для setup/teardown.
- **Mocks для DbContext** — используй SQLite in-memory или Testcontainers.
- **Тестирование через reflection в generated code** — используй CLI `dotnet avto handlers inspect`.
- **Игнорирование golden test failures** — обновление golden файлов должно быть intentional.
