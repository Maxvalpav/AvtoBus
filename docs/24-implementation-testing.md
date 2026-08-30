# 🧪 Реализация: Тест-харнесс и виртуальное время

> **Design draft.** Harness и Testcontainers ниже задают требуемое поведение; реальные тестовые проекты пока не созданы.

Пакет `AvtoBus.Testing`, совместим с xUnit/NUnit/MSTest и Bogus.

## 1. Интерфейс харнесса

```csharp
// AvtoBus.Testing/AvtoBusTestHarness.cs
public sealed class AvtoBusTestHarness : IAsyncDisposable
{
    public IBus Bus { get; }
    public FakeTimeProvider Clock { get; } = new();
    public TestTransport Transport { get; }
    public IServiceProvider Services { get; }
    public InMemorySagaStore Sagas { get; } = new();

    private readonly IServiceScope _root;
    private readonly BusHost _host;

    private AvtoBusTestHarness(Action<IServiceCollection, BusOptions>? configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(Clock);
        services.AddAvtoBus(b =>
        {
            b.UseInMemory();
            b.AddTestTransport();
            configure?.Invoke(services, b);
        });
        services.AddSingleton<ISagaStore>(Sagas);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateScopes = true,
            ValidateOnBuild = true,
        });

        Services = provider;
        _root = provider.CreateScope();
        Bus = _root.ServiceProvider.GetRequiredService<IBus>();
        Transport = _root.ServiceProvider.GetRequiredService<TestTransport>();
        _host = _root.ServiceProvider.GetRequiredService<BusHost>();
    }

    public static ValueTask<AvtoBusTestHarness> CreateAsync(
        Action<IServiceCollection>? configureServices = null,
        Action<BusOptions>? configureBus = null)
    {
        var h = new AvtoBusTestHarness((s, b) =>
        {
            configureServices?.Invoke(s);
            configureBus?.Invoke(b);
        });
        return h.StartAsync();
    }

    private async ValueTask<AvtoBusTestHarness> StartAsync()
    {
        await _host.StartAsync(CancellationToken.None);
        return this;
    }

    // Хелперы

    public IEnumerable<TransportMessage> Consumed<T>() where T : class
        => Transport.Consumed.Where(m => m.Envelope.MessageType == TypeAliases.Get(typeof(T)));

    public IEnumerable<object> Published<T>() where T : class
        => Transport.Published.Where(m => m.MessageType == TypeAliases.Get(typeof(T)))
                              .Select(m => m.Payload!);

    public async Task WaitForConsumed<T>(TimeSpan timeout) where T : class
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (Consumed<T>().Any()) return;
            await Transport.OnMessage.WaitToReadAsync(cts.Token);
        }
        throw new TimeoutException($"No Consumed<{typeof(T).Name}> within {timeout}");
    }

    public async ValueTask AdvanceTime(TimeSpan delta)
    {
        Clock.Advance(delta);
        await Transport.DrainAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync(CancellationToken.None);
        if (_root is IAsyncDisposable d) await d.DisposeAsync();
    }
}
```

## 2. TestTransport — InMemory с ловлей всех сообщений

```csharp
public sealed class TestTransport : ITransport
{
    public string Name => "test";

    private readonly InMemoryTransport _inner = new();
    public Channel<TransportMessage> OnMessage { get; } =
        Channel.CreateUnbounded<TransportMessage>();

    public List<(object? Payload, Envelope Envelope, string Destination)> Published { get; } = new();
    public List<TransportMessage> Consumed { get; } = new();

    public async ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct)
    {
        object? payload = null;
        try { payload = System.Text.Json.JsonSerializer.Deserialize(
            envelope.Body, TypeLoader.Get(envelope.MessageType)); } catch { }
        lock (Published) Published.Add((payload, envelope, dest.Address));

        await _inner.SendAsync(envelope, dest, ct);
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var m in _inner.ReceiveAsync(sub, ct))
        {
            lock (Consumed) Consumed.Add(m);
            OnMessage.Writer.TryWrite(m);
            yield return new TransportMessage(m.Envelope, new TestAck(m.Ack));
        }
    }

    public async Task DrainAsync()
    {
        // Ждём, пока все поставленные сообщения дойдут до консьюмеров
        await _inner.DrainAsync(CancellationToken.None);
    }
}

internal sealed class TestAck(IAckContext inner) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default) => inner.AckAsync(ct);
    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default) => inner.NackAsync(requeue, ct);
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default) => inner.DeferAsync(delay, ct);
}
```

## 3. FakeTimeProvider + виртуальные часы

```csharp
// .NET 8+: встроенный FakeTimeProvider, но расширяем его
public static class TimeProviderExtensions
{
    public static async ValueTask AdvanceUntilScheduled(this FakeTimeProvider clock,
        AvtoBusTestHarness h, TimeSpan? maxJump = null)
    {
        var next = await h.Transport.NextScheduledAt();
        if (next is null) return;
        var delta = next.Value - clock.GetUtcNow();
        if (maxJump is { } mx && delta > mx) delta = mx;
        if (delta > TimeSpan.Zero)
            await h.AdvanceTime(delta);
    }
}
```

Использование в тестах саг/ретраев:

```csharp
[Fact]
public async Task Payment_timeout_cancels_order()
{
    await using var h = await AvtoBusTestHarness.CreateAsync(s =>
    {
        s.AddSaga<OrderSaga, OrderSagaState>();
    });

    await h.Bus.Publish(new OrderPlaced(Id, 100m));
    await h.WaitForConsumed<OrderPlaced>(5.Seconds());

    // Прыгаем на 35 минут вперёд в виртуальном времени — срабатывает timeout без реального ожидания
    await h.AdvanceTime(TimeSpan.FromMinutes(35));
    await h.Transport.DrainAsync();

    // Сага должна опубликовать OrderCancelled
    h.Published<OrderCancelled>().Should().ContainSingle(c => c.OrderId == Id);
}
```

## 4. Faker (Bogus-интеграция)

```csharp
// AvtoBus.Testing/Fakes/ContractFaker.cs
public static class ContractFaker
{
    private static readonly Faker F = new("ru");

    public static PlaceOrder PlaceOrder() => new(
        OrderId: Guid.CreateVersion7(),
        CustomerId: F.Random.Guid().ToString(),
        Items:
        [
            new OrderItem(F.Commerce.Ean13(), F.Random.Int(1, 5), F.Random.Decimal(100, 5000))
        ]);

    public static IEnumerable<PlaceOrder> PlaceOrders(int n) =>
        Enumerable.Range(0, n).Select(_ => PlaceOrder());
}
```

## 5. Контракт-тесты consumer-driven (Pact-style)

```csharp
// AvtoBus.Testing/Contract/ContractVerifier.cs
public sealed class ContractVerifier
{
    public static async Task VerifyAsync<THandler, TMessage>(
        IEnumerable<SampleEnvelope> publishedSamples,
        Action<AssertionBuilder>? assert = null,
        CancellationToken ct = default)
        where THandler : class
    {
        await using var h = await AvtoBusTestHarness.CreateAsync(s =>
        {
            s.AddConsumer<THandler>();
        });

        foreach (var sample in publishedSamples)
        {
            await h.Transport.DeliverDirect(sample);
            try
            {
                // Проверяем: 1) десериализуется, 2) хендлер не бросает неожиданного, 3) каскадные сообщения валидны
            }
            catch (Exception ex)
            {
                throw new ContractViolationException(
                    $"Handler {typeof(THandler).Name} rejects sample {sample.MessageId}", ex);
            }
        }
    }
}
```

## 6. Snapshot-тесты каскадов (Verify-интеграция)

```csharp
[UsesVerify]
public class CascadesSnapshot
{
    [Fact]
    public Task PlaceOrder_cascades() => VerifyCascade.For<PlaceOrder>(
        setup: s => { /* моки репозитория */ },
        message: ContractFaker.PlaceOrder());
}
```

Генерирует снапшот `.verified.txt`:

```
→ PlaceOrder (orderId=..., customerId=..., items=1)
  ← Publish OrderPlaced { orderId=..., total=... }
```

## 7. Хаос-мидлварь для staging/тестов

```csharp
// AvtoBus.Testing/Chaos/ChaosMiddleware.cs
public sealed class ChaosMiddleware : IBusMiddleware
{
    private readonly ChaosOptions _opt;
    private readonly ThreadLocal<Random> _rng = new(() => new Random(Guid.NewGuid().GetHashCode()));

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (_opt.DuplicateProbability > 0 && _rng.Value!.NextDouble() < _opt.DuplicateProbability)
            await next(ctx); // двойной вызов = эмуляция дублей

        if (_opt.ReorderProbability > 0 && _rng.Value.NextDouble() < _opt.ReorderProbability)
            await Task.Delay(_opt.ReorderMaxDelay, ctx.CancellationToken);

        if (_opt.FailProbability > 0 && _rng.Value.NextDouble() < _opt.FailProbability)
            throw new ChaosInjectedException("injected");

        await next(ctx);
    }
}

public sealed record ChaosOptions(
    double DuplicateProbability = 0,
    double ReorderProbability = 0,
    double FailProbability = 0,
    TimeSpan ReorderMaxDelay = default);
```

Подключение в staging:

```csharp
if (builder.Environment.IsStaging())
    bus.Pipeline(p => p.UseChaos(new(0.05, 0.1, 0.02, TimeSpan.FromSeconds(1))));
```

## 8. Идемпотентность-тест

```csharp
// AvtoBus.Testing/Idempotency/IdempotencyVerifier.cs
public static class Idempotency
{
    public static async Task Verify<THandler, TMsg>(TMsg message)
        where THandler : class
    {
        var mockRepo = new Mock<IOrderRepo>();
        // ... настраиваем side-effect моки

        await using var h1 = await AvtoBusTestHarness.CreateAsync(s =>
        {
            s.AddConsumer<THandler>();
            s.AddSingleton(mockRepo.Object);
        });
        await h1.Bus.Send((object)message);
        await h1.Transport.DrainAsync();

        await using var h2 = await AvtoBusTestHarness.CreateAsync(s =>
        {
            s.AddConsumer<THandler>();
            s.AddSingleton(mockRepo.Object);
        });
        await h2.Bus.Send((object)message);
        await h2.Transport.DrainAsync();

        // Проверяем, что side-effect не произошёл дважды
        mockRepo.Verify(r => r.Add(It.IsAny<Order>()), Times.Once);
    }
}
```

## 9. Property-based тестирование с FsCheck

```csharp
public class OrderProperties
{
    [Property]
    public Property Handler_never_returns_null_for_nonempty_order(OrderItem[] items)
    {
        if (items.Length == 0) return true.ToProperty(); // отдельно тестируем валидацию

        var cmd = new PlaceOrder(Guid.NewGuid(), "c", items);
        var result = OrderHandlers.Handle(cmd, new FakeRepo(), default).Result;
        return (result is not null).ToProperty();
    }

    [Property]
    public Property Total_equals_sum_of_items(PlaceOrder cmd)
    {
        var expected = cmd.Items.Sum(i => i.Price * i.Qty);
        var r = OrderHandlers.Handle(cmd, new FakeRepo(), default).Result;
        return (r.Total == expected).ToProperty();
    }
}
```

## 10. Интеграционные тесты с Testcontainers

```csharp
// AvtoBus.Testing.Containers/RabbitMqFixture.cs
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly TestcontainersContainer _rabbit = new TestcontainersBuilder<TestcontainersContainer>()
        .WithImage("rabbitmq:4-management-alpine")
        .WithPortBinding(5672, true)
        .WithPortBinding(15672, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(5672))
        .Build();

    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        await _rabbit.StartAsync();
        var port = _rabbit.GetMappedPublicPort(5672);
        ConnectionString = $"amqp://guest:guest@localhost:{port}";
    }

    public Task DisposeAsync() => _rabbit.DisposeAsync().AsTask();
}
```

Использование:

```csharp
public class RabbitIntegrationTests : IClassFixture<RabbitMqFixture>
{
    private readonly RabbitMqFixture _rabbit;
    public RabbitIntegrationTests(RabbitMqFixture rabbit) => _rabbit = rabbit;

    [Fact]
    public async Task End_to_end_delivers()
    {
        await using var provider = new ServiceCollection()
            .AddAvtoBus(b =>
            {
                b.UseRabbitMq(_rabbit.ConnectionString);
                b.AddConsumersFromAssembly(typeof(RabbitIntegrationTests).Assembly);
            })
            .BuildServiceProvider();

        var bus = provider.GetRequiredService<IBus>();
        await bus.Publish(new OrderPlaced(Guid.NewGuid(), "c1", 100m));
        // ...
    }
}
```

## 11. Лучшие практики для тестов AvtoBus

1. **Юнит-тесты хендлеров** — вызывайте метод хендлера напрямую, без шины (рукописные Arrange/Act/Assert).
2. **Потоковые тесты** — используйте `AvtoBusTestHarness`, проверяйте каскады (`h.Published<T>()`).
3. **Саги** — используйте виртуальное время (`FakeTimeProvider.Advance`), не `Task.Delay` в тестах.
4. **Интеграционные** — Testcontainers + conformance-kit (идея 98).
5. **Нагрузочные** — BenchmarkDotNet в проекте `AvtoBus.Benchmarks`.
6. **Хаос** — включайте `ChaosMiddleware` на staging постоянно, а не в дедлайн.
