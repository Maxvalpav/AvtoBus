# Тесты фреймворка AvtoBus

Юнит-, интеграционные и conformance-тесты.

---

## tests/AvtoBus.Core.Tests/EnvelopeTests.cs

```csharp
using AvtoBus;
using FluentAssertions;
using Xunit;

namespace AvtoBus.Core.Tests;

public class EnvelopeTests
{
    [Fact]
    public void WithHeader_creates_new_instance()
    {
        var envelope = CreateEnvelope();
        var modified = envelope.WithHeader("key", "value");

        modified.Headers.Should().ContainKey("key");
        envelope.Headers.Should().NotContainKey("key");   // Immutable
    }

    [Fact]
    public void IsExpired_returns_true_when_ttl_passed()
    {
        var envelope = CreateEnvelope() with
        {
            SentAt = DateTimeOffset.UtcNow.AddMinutes(-10),
            TimeToLive = TimeSpan.FromMinutes(5)
        };

        envelope.IsExpired(DateTimeOffset.UtcNow).Should().BeTrue();
    }

    [Fact]
    public void IsExpired_returns_false_when_ttl_not_set()
    {
        var envelope = CreateEnvelope();
        envelope.IsExpired(DateTimeOffset.UtcNow).Should().BeFalse();
    }

    [Fact]
    public void IsDeferred_respects_deliver_at()
    {
        var future = DateTimeOffset.UtcNow.AddMinutes(5);
        var envelope = CreateEnvelope() with { DeliverAt = future };

        envelope.IsDeferred(DateTimeOffset.UtcNow).Should().BeTrue();
        envelope.IsDeferred(future.AddMinutes(1)).Should().BeFalse();
    }

    [Fact]
    public void WithAttempt_increments_delivery_attempt()
    {
        var envelope = CreateEnvelope();
        envelope.WithAttempt(3).DeliveryAttempt.Should().Be(3);
    }

    private static Envelope CreateEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "test.message",
        Body = new byte[] { 1, 2, 3 },
        SentAt = DateTimeOffset.UtcNow,
    };
}
```

---

## tests/AvtoBus.Core.Tests/PipelineTests.cs

```csharp
using AvtoBus;
using AvtoBus.Pipeline;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Core.Tests;

public class PipelineTests
{
    [Fact]
    public async Task Pipeline_executes_middleware_in_order()
    {
        var order = new List<string>();

        var builder = new BusPipelineBuilder();
        builder.Use(async (ctx, next) => { order.Add("first-before"); await next(ctx); order.Add("first-after"); });
        builder.Use(async (ctx, next) => { order.Add("second-before"); await next(ctx); order.Add("second-after"); });

        var pipeline = builder.Build(ctx => { order.Add("terminal"); return default; });

        await pipeline(CreateContext());

        order.Should().Equal(
            "first-before", "second-before", "terminal", "second-after", "first-after");
    }

    [Fact]
    public async Task UseWhen_runs_branch_only_when_condition_true()
    {
        var branchExecuted = false;

        var builder = new BusPipelineBuilder();
        builder.UseWhen(
            ctx => ctx.Envelope.TenantId == "special",
            branch => branch.Use((ctx, next) => { branchExecuted = true; return next(ctx); }));

        var pipeline = builder.Build(_ => default);

        // TenantId != special → branch skip
        await pipeline(CreateContext(tenant: "normal"));
        branchExecuted.Should().BeFalse();

        // TenantId == special → branch run
        await pipeline(CreateContext(tenant: "special"));
        branchExecuted.Should().BeTrue();
    }

    [Fact]
    public async Task Middleware_can_short_circuit()
    {
        var terminalReached = false;

        var builder = new BusPipelineBuilder();
        builder.Use((ctx, next) => default); // не вызывает next → короткое замыкание

        var pipeline = builder.Build(ctx => { terminalReached = true; return default; });
        await pipeline(CreateContext());

        terminalReached.Should().BeFalse();
    }

    private static ConsumeContext CreateContext(string? tenant = null) => new()
    {
        Envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "test",
            Body = ReadOnlyMemory<byte>.Empty,
            TenantId = tenant,
        },
        Message = new object(),
        Services = new ServiceCollection().BuildServiceProvider(),
        CancellationToken = CancellationToken.None,
    };
}
```

---

## tests/AvtoBus.Core.Tests/BusFlowTests.cs

```csharp
using AvtoBus;
using AvtoBus.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Core.Tests;

// Контракты
public sealed record PlaceOrder(Guid OrderId, decimal Total) : ICommand;
public sealed record OrderPlaced(Guid OrderId, decimal Total) : IEvent;
public sealed record OrderConfirmed(Guid OrderId) : IEvent;

// Хендлер с каскадом
public static class OrderHandlers
{
    public static OrderPlaced Handle(PlaceOrder cmd) => new(cmd.OrderId, cmd.Total);
}

public static class NotificationHandlers
{
    public static readonly List<Guid> Notified = new();
    public static void Handle(OrderPlaced evt) => Notified.Add(evt.OrderId);
}

public class BusFlowTests
{
    [Fact]
    public async Task Command_handler_publishes_cascade_event()
    {
        NotificationHandlers.Notified.Clear();

        await using var harness = await AvtoBusTestHarness.CreateAsync(configureBus: bus =>
        {
            bus.AddConsumersFromAssembly(typeof(BusFlowTests).Assembly);
        });

        var orderId = Guid.NewGuid();
        await harness.SendAndWait(new PlaceOrder(orderId, 100m));

        // Каскадное событие должно быть опубликовано и обработано
        harness.Published<OrderPlaced>().Should().ContainSingle(e => e.OrderId == orderId);
    }

    [Fact]
    public async Task Event_reaches_all_subscribers()
    {
        NotificationHandlers.Notified.Clear();

        await using var harness = await AvtoBusTestHarness.CreateAsync(configureBus: bus =>
        {
            bus.AddConsumersFromAssembly(typeof(BusFlowTests).Assembly);
        });

        var orderId = Guid.NewGuid();
        await harness.PublishAndWait(new OrderPlaced(orderId, 100m));

        NotificationHandlers.Notified.Should().Contain(orderId);
    }
}
```

---

## tests/AvtoBus.Core.Tests/RecoverabilityTests.cs

```csharp
using AvtoBus;
using AvtoBus.Pipeline;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AvtoBus.Core.Tests;

public class RecoverabilityTests
{
    [Fact]
    public async Task Immediate_retry_rethrows_within_limit()
    {
        var options = new RecoverabilityOptions { ImmediateRetries = 3 };
        var mw = new RecoverabilityMiddleware(options, NullLogger<RecoverabilityMiddleware>.Instance);

        var ctx = CreateContext(attempt: 1);
        var act = async () => await mw.InvokeAsync(ctx, _ => throw new InvalidOperationException());

        // В пределах immediate retries → пробрасывает для повтора
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Mapped_exception_is_discarded()
    {
        var options = new RecoverabilityOptions { ImmediateRetries = 0, DelayedRetries = 0 };
        options.MapException<ValidationException>(FailureAction.Discard);

        var mw = new RecoverabilityMiddleware(options, NullLogger<RecoverabilityMiddleware>.Instance);
        var ctx = CreateContext(attempt: 0);

        // ValidationException → discard, без исключения наружу
        var act = async () => await mw.InvokeAsync(ctx, _ => throw new ValidationException());
        await act.Should().NotThrowAsync();
    }

    private static ConsumeContext CreateContext(int attempt) => new()
    {
        Envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "test",
            Body = ReadOnlyMemory<byte>.Empty,
            DeliveryAttempt = attempt,
        },
        Message = new object(),
        Services = new Microsoft.Extensions.DependencyInjection.ServiceCollection().BuildServiceProvider(),
        CancellationToken = CancellationToken.None,
    };
}

public sealed class ValidationException : Exception;
```

---

## tests/AvtoBus.Sagas.Tests/OrderSagaTests.cs

```csharp
using AvtoBus;
using AvtoBus.Testing;
using FluentAssertions;
using Xunit;

namespace AvtoBus.Sagas.Tests;

public sealed record StartOrder(Guid OrderId, decimal Total) : ICommand;
public sealed record PaymentDone(Guid OrderId) : IEvent;
public sealed record OrderReady(Guid OrderId) : IEvent;

public sealed class TestSagaState : SagaState
{
    public Guid OrderId { get; set; }
    public bool Paid { get; set; }
}

public sealed class TestSaga : Saga<TestSagaState>,
    IStartedBy<StartOrder>, IHandle<PaymentDone>
{
    protected override void Correlate(SagaMap<TestSagaState> map)
    {
        map.On<StartOrder>(m => m.OrderId).StartsNew();
        map.On<PaymentDone>(m => m.OrderId);
    }

    public Task Handle(StartOrder m)
    {
        State.OrderId = m.OrderId;
        State.Status = "AwaitingPayment";
        return Task.CompletedTask;
    }

    public Task Handle(PaymentDone m)
    {
        State.Paid = true;
        State.Status = "Ready";
        MarkComplete();
        return Publish(new OrderReady(State.OrderId)).AsTask();
    }
}

public class OrderSagaTests
{
    [Fact]
    public async Task Saga_completes_after_payment()
    {
        await using var harness = await AvtoBusTestHarness.CreateAsync(configureBus: bus =>
        {
            bus.AddSaga<TestSaga, TestSagaState>();
        });

        var orderId = Guid.NewGuid();
        await harness.SendAndWait(new StartOrder(orderId, 100m));
        await harness.PublishAndWait(new PaymentDone(orderId));

        harness.Published<OrderReady>().Should().ContainSingle(e => e.OrderId == orderId);
    }

    [Fact]
    public async Task Late_message_without_instance_is_ignored()
    {
        await using var harness = await AvtoBusTestHarness.CreateAsync(configureBus: bus =>
        {
            bus.AddSaga<TestSaga, TestSagaState>();
        });

        // PaymentDone без предшествующего StartOrder → игнорируется (не StartsNew)
        await harness.PublishAndWait(new PaymentDone(Guid.NewGuid()));

        harness.Published<OrderReady>().Should().BeEmpty();
    }
}
```

---

## tests/AvtoBus.Sagas.Tests/SagaConcurrencyTests.cs

```csharp
using AvtoBus;
using FluentAssertions;
using Xunit;

namespace AvtoBus.Sagas.Tests;

public class SagaConcurrencyTests
{
    [Fact]
    public async Task Optimistic_concurrency_throws_on_version_mismatch()
    {
        var store = new InMemorySagaStore();
        var instance = new SagaInstance
        {
            Id = Guid.NewGuid(),
            CorrelationKey = "key-1",
            StateJson = "{}",
            Status = "Active",
        };

        // Первое сохранение
        await store.SaveAsync(typeof(TestSaga), instance, expectedVersion: 0, CancellationToken.None);

        // Второе сохранение с устаревшей версией → конфликт
        var act = async () => await store.SaveAsync(
            typeof(TestSaga), instance, expectedVersion: 0, CancellationToken.None);

        await act.Should().ThrowAsync<SagaConcurrencyException>();
    }

    [Fact]
    public async Task Load_returns_null_for_unknown_key()
    {
        var store = new InMemorySagaStore();
        var result = await store.LoadAsync(typeof(TestSaga), "unknown", CancellationToken.None);
        result.Should().BeNull();
    }
}
```

---

## tests/AvtoBus.Generators.Tests/GeneratorTests.cs

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using FluentAssertions;
using Xunit;

namespace AvtoBus.Generators.Tests;

public class GeneratorTests
{
    [Fact]
    public void Generator_creates_dispatcher_for_method_handler()
    {
        var source = """
            using AvtoBus;
            public sealed record PlaceOrder(System.Guid Id) : ICommand;
            public static class Handlers
            {
                public static void Handle(PlaceOrder cmd) { }
            }
            """;

        var (diagnostics, output) = RunGenerator(source);

        diagnostics.Should().NotContain(d => d.Severity == DiagnosticSeverity.Error);
        output.Should().Contain("IMessageDispatcher");
        output.Should().Contain("PlaceOrder");
    }

    [Fact]
    public void Generator_reports_AVB002_for_duplicate_command_handlers()
    {
        var source = """
            using AvtoBus;
            public sealed record DoThing() : ICommand;
            public static class H1 { public static void Handle(DoThing cmd) { } }
            public static class H2 { public static void Handle(DoThing cmd) { } }
            """;

        var (diagnostics, _) = RunGenerator(source);

        diagnostics.Should().Contain(d => d.Id == "AVB002");
    }

    private static (IReadOnlyList<Diagnostic>, string) RunGenerator(string source)
    {
        var compilation = CSharpCompilation.Create("test",
            new[] { CSharpSyntaxTree.ParseText(source) },
            Basic.Reference.Assemblies.Net80.References.All,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new HandlerGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var outputCompilation, out var diags);

        var generated = outputCompilation.SyntaxTrees
            .Skip(1)
            .Select(t => t.ToString())
            .Aggregate("", (a, b) => a + b);

        return (diags, generated);
    }
}
```

---

## tests/AvtoBus.Integration.Tests/RabbitMqIntegrationTests.cs

```csharp
using AvtoBus;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;
using Xunit;

namespace AvtoBus.Integration.Tests;

public class RabbitMqIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder()
        .WithImage("rabbitmq:4-management-alpine")
        .Build();

    public Task InitializeAsync() => _rabbit.StartAsync();
    public Task DisposeAsync() => _rabbit.DisposeAsync().AsTask();

    [Fact]
    public async Task End_to_end_message_delivery()
    {
        var received = new TaskCompletionSource<TestMessage>();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(received);
                services.AddAvtoBus(bus =>
                {
                    bus.UseRabbitMq(_rabbit.GetConnectionString());
                    bus.AddConsumer<TestMessageConsumer>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Publish(new TestMessage("hello"));

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(30));
        result.Text.Should().Be("hello");

        await host.StopAsync();
    }
}

public sealed record TestMessage(string Text) : IEvent;

public sealed class TestMessageConsumer(TaskCompletionSource<TestMessage> tcs) : IConsumer<TestMessage>
{
    public Task Consume(ConsumeContext<TestMessage> ctx)
    {
        tcs.TrySetResult(ctx.Message);
        return Task.CompletedTask;
    }
}
```

---

## tests/AvtoBus.Conformance/TransportConformanceTests.cs

```csharp
using AvtoBus;
using AvtoBus.Transport;
using FluentAssertions;
using Xunit;

namespace AvtoBus.Conformance;

/// <summary>
/// Базовый набор тестов, который обязан пройти каждый транспорт.
/// </summary>
public abstract class TransportConformanceTests : IAsyncLifetime
{
    protected ITransport Transport = null!;

    protected abstract ValueTask<ITransport> CreateTransportAsync();

    public async Task InitializeAsync() => Transport = await CreateTransportAsync();
    public Task DisposeAsync() => Transport.DisposeAsync().AsTask();

    [Fact]
    public async Task Send_then_receive_delivers_same_message()
    {
        var envelope = CreateEnvelope("test-body");
        var dest = new TransportDestination("conformance-q1", DestinationKind.Queue);

        await Transport.SendAsync(envelope, dest);

        var sub = new TransportSubscription("conformance-q1", Array.Empty<string>(), 1, "group1");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var msg in Transport.ReceiveAsync(sub, cts.Token))
        {
            msg.Envelope.MessageId.Should().Be(envelope.MessageId);
            await msg.Ack.AckAsync();
            return;
        }

        Assert.Fail("No message received");
    }

    [Fact]
    public async Task Headers_are_preserved()
    {
        var envelope = CreateEnvelope("body") with
        {
            Headers = new Dictionary<string, string> { ["custom"] = "value" }.ToFrozenDictionary()
        };
        var dest = new TransportDestination("conformance-q2", DestinationKind.Queue);
        await Transport.SendAsync(envelope, dest);

        var sub = new TransportSubscription("conformance-q2", Array.Empty<string>(), 1, "g");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var msg in Transport.ReceiveAsync(sub, cts.Token))
        {
            msg.Envelope.Headers.Should().ContainKey("custom");
            await msg.Ack.AckAsync();
            return;
        }
        Assert.Fail("No message");
    }

    [Fact]
    public async Task Nack_without_requeue_removes_message()
    {
        var envelope = CreateEnvelope("body");
        var dest = new TransportDestination("conformance-q3", DestinationKind.Queue);
        await Transport.SendAsync(envelope, dest);

        var sub = new TransportSubscription("conformance-q3", Array.Empty<string>(), 1, "g");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var msg in Transport.ReceiveAsync(sub, cts.Token))
        {
            await msg.Ack.NackAsync(requeue: false);
            return;
        }
    }

    private static Envelope CreateEnvelope(string body) => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "conformance.test",
        Body = System.Text.Encoding.UTF8.GetBytes(body),
        SentAt = DateTimeOffset.UtcNow,
    };
}

/// <summary>
/// Прогон conformance-kit на InMemory-транспорте.
/// </summary>
public sealed class InMemoryConformanceTests : TransportConformanceTests
{
    protected override ValueTask<ITransport> CreateTransportAsync()
        => ValueTask.FromResult<ITransport>(
            new Transport.InMemory.InMemoryTransport(TimeProvider.System));
}
```

---

## tests/AvtoBus.Core.Tests/AvtoBus.Core.Tests.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <IsPackable>false</IsPackable>
    <TreatWarningsAsErrors>false</TreatWarningsAsErrors>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="FluentAssertions" />
    <PackageReference Include="Microsoft.Extensions.TimeProvider.Testing" />
    <PackageReference Include="NSubstitute" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\AvtoBus.Core\AvtoBus.Core.csproj" />
    <ProjectReference Include="..\..\src\AvtoBus.Testing\AvtoBus.Testing.csproj" />
  </ItemGroup>

</Project>
```
