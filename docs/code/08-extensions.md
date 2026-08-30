# AvtoBus — DI Registration и расширения

> **Code sketch / unverified.** Регистрации DI ещё не проверены через `ValidateScopes=true`. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus/ServiceCollectionExtensions.cs

```csharp
using AvtoBus.Dispatching;
using AvtoBus.Pipeline;
using AvtoBus.Transport;
using AvtoBus.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus;

public static class AvtoBusServiceCollectionExtensions
{
    /// <summary>
    /// Добавить AvtoBus с конфигурацией.
    /// </summary>
    public static IServiceCollection AddAvtoBus(
        this IServiceCollection services,
        Action<BusOptions> configure)
    {
        var options = new BusOptions(services);
        configure(options);

        // ── TimeProvider ──
        services.TryAddSingleton(TimeProvider.System);

        // ── Core services ──
        services.TryAddSingleton<IBusContextAccessor, AsyncLocalBusContextAccessor>();
        services.TryAddSingleton<IBusState, InMemoryBusState>();
        services.TryAddSingleton<IDeferralSink, InMemoryDeferralSink>();
        services.TryAddSingleton<ITenantAccessor, AsyncLocalTenantAccessor>();

        // ── Default transport: InMemory ──
        services.TryAddSingleton<ITransport>(sp =>
            new InMemoryTransport(sp.GetRequiredService<TimeProvider>()));
        services.TryAddSingleton<ITransportSelector, InMemoryTransportSelector>();

        // ── Router ──
        services.TryAddSingleton<IRouter>(sp =>
            new ConventionRouter(options.Routes));

        // ── Serializer ──
        services.TryAddSingleton<ISerializer, DefaultJsonSerializer>();
        services.TryAddSingleton<ITypeResolver>(sp =>
        {
            var dispatchers = sp.GetRequiredService<IEnumerable<IMessageDispatcher>>();
            return new TypeAliasResolver(dispatchers);
        });

        // ── Pipeline ──
        services.AddSingleton(sp =>
        {
            var builder = new BusPipelineBuilder();
            options.Pipeline.Configure(builder);
            if (builder.Count == 0)
                ApplyDefaultPipeline(builder);
            return builder;
        });

        // ── Handler invoker ──
        services.TryAddSingleton<HandlerInvokerMiddleware>();

        // ── Standard middleware ──
        services.TryAddSingleton<TelemetryMiddleware>();
        services.TryAddSingleton<ScopeMiddleware>();
        services.TryAddSingleton<TenantMiddleware>();
        services.TryAddSingleton<RecoverabilityMiddleware>(sp =>
            new RecoverabilityMiddleware(
                options.Recoverability,
                sp.GetRequiredService<ILogger<RecoverabilityMiddleware>>()));

        if (options.InboxOptions is not null)
        {
            services.TryAddSingleton(new InboxDedupMiddleware(
                options.InboxOptions,
                sp.GetService<IInMemoryCache>()));
        }

        // ── Dispatchers ──
        services.AddSingleton(sp =>
        {
            var list = new List<IMessageDispatcher>();
            list.AddRange(ReflectionDispatcherBuilder.BuildFromAssemblies(
                options.ConsumerAssemblies, sp));
            list.AddRange(ReflectionDispatcherBuilder.BuildFromTypes(
                options.ConsumerTypes, sp));
            return new DispatcherRegistry(list);
        });

        // ── Sagas ──
        services.TryAddSingleton<ISagaStore, InMemorySagaStore>();
        foreach (var sagaConfig in options.Sagas)
        {
            services.AddSaga(sagaConfig);
        }

        // ── Metrics ──
        services.AddSingleton(BusMetrics.Meter);

        // ── Hosted service ──
        services.AddHostedService<BusHost>();

        // ── Store options ──
        services.AddSingleton(options);

        return services;
    }

    /// <summary>
    /// Добавить RabbitMQ транспорт.
    /// </summary>
    public static BusOptions UseRabbitMq(this BusOptions options, string connectionString)
    {
        options.DefaultTransport = "rabbitmq";
        options.DefaultConnectionString = connectionString;

        options.Services.AddSingleton<ITransport>(sp =>
            new RabbitMqTransport(connectionString, sp.GetRequiredService<ILogger<RabbitMqTransport>>()));

        return options;
    }

    /// <summary>
    /// Добавить Kafka транспорт.
    /// </summary>
    public static BusOptions UseKafka(this BusOptions options, string bootstrapServers)
    {
        options.DefaultTransport = "kafka";

        options.Services.AddSingleton<ITransport>(sp =>
            new KafkaTransport(
                bootstrapServers,
                sp.GetRequiredService<ILogger<KafkaTransport>>()));

        return options;
    }

    /// <summary>
    /// Добавить сагу.
    /// </summary>
    public static void AddSaga(this IServiceCollection services, SagaConfiguration config)
    {
        var sagaMiddlewareType = typeof(SagaMiddleware<,>)
            .MakeGenericType(config.SagaType, config.StateType);
        services.AddSingleton(typeof(IBusMiddleware), sagaMiddlewareType);
    }

    private static void ApplyDefaultPipeline(BusPipelineBuilder builder)
    {
        builder.Use<TelemetryMiddleware>();
        builder.Use<ScopeMiddleware>();
        builder.Use<TenantMiddleware>();
        builder.Use<RecoverabilityMiddleware>();
    }
}
```

---

## AvtoBus/BusExtensions.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Extension-методы для удобной работы с шиной.
/// </summary>
public static class BusExtensions
{
    /// <summary>
    /// Опубликовать с задержкой.
    /// </summary>
    public static ValueTask PublishAfter<T>(this IBus bus, T @event, TimeSpan delay,
        CancellationToken ct = default) where T : class
        => bus.Publish(@event, new PublishOptions { Delay = delay }, ct);

    /// <summary>
    /// Опубликовать с partition key.
    /// </summary>
    public static ValueTask Publish<T>(this IBus bus, T @event, string partitionKey,
        CancellationToken ct = default) where T : class
        => bus.Publish(@event, new PublishOptions { PartitionKey = partitionKey }, ct);

    /// <summary>
    /// Отправить команду с заголовками.
    /// </summary>
    public static ValueTask Send<T>(this IBus bus, T command,
        Dictionary<string, string> headers,
        CancellationToken ct = default) where T : class
        => bus.Send(command, new SendOptions { Headers = headers }, ct);

    /// <summary>
    /// Request с типизированным timeout.
    /// </summary>
    public static ValueTask<TReply> Request<T, TReply>(this IBus bus, T request,
        int timeoutSeconds, CancellationToken ct = default)
        where T : class where TReply : class
        => bus.Request<T, TReply>(request, TimeSpan.FromSeconds(timeoutSeconds), ct);

    /// <summary>
    /// Schedule через TimeSpan.
    /// </summary>
    public static ValueTask<Guid> Schedule<T>(this IBus bus, T message,
        TimeSpan delay, CancellationToken ct = default) where T : class
        => bus.Schedule(message, delay, ct);
}

/// <summary>
/// Extension для TimeSpan.
/// </summary>
public static class TimeSpanExtensions
{
    public static TimeSpan Milliseconds(this int ms) => TimeSpan.FromMilliseconds(ms);
    public static TimeSpan Seconds(this int s) => TimeSpan.FromSeconds(s);
    public static TimeSpan Minutes(this int m) => TimeSpan.FromMinutes(m);
    public static TimeSpan Hours(this int h) => TimeSpan.FromHours(h);
    public static TimeSpan Days(this int d) => TimeSpan.FromDays(d);
}
```

---

## AvtoBus/RecoveryMiddleware.cs

```csharp
using AvtoBus.Pipeline;

namespace AvtoBus.Pipeline;

/// <summary>
/// Middleware для recoverability: классификация ошибок и маршрутизация.
/// </summary>
internal sealed class RecoverabilityMiddleware : IBusMiddleware
{
    private readonly RecoverabilityOptions _options;
    private readonly ILogger<RecoverabilityMiddleware> _log;

    public RecoverabilityMiddleware(
        RecoverabilityOptions options,
        ILogger<RecoverabilityMiddleware> log)
    {
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        try
        {
            await next(ctx);
        }
        catch (DeadLetterException)
        {
            throw; // Уже в DLQ
        }
        catch (OperationCanceledException) when (ctx.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var attempt = ctx.Attempt;

            // Immediate retry?
            if (attempt < _options.ImmediateRetries)
            {
                _log.LogWarning(ex, "Immediate retry {Attempt}/{Max} for {Type}",
                    attempt + 1, _options.ImmediateRetries, ctx.Envelope.MessageType);
                throw;
            }

            // Delayed retry?
            var delayedAttempt = attempt - _options.ImmediateRetries;
            if (delayedAttempt < _options.DelayedRetries)
            {
                var delay = CalculateBackoff(delayedAttempt);
                _log.LogWarning(ex, "Delayed retry {Attempt}/{Max} in {Delay} for {Type}",
                    delayedAttempt + 1, _options.DelayedRetries, delay, ctx.Envelope.MessageType);

                await ctx.DeferAsync(delay, ctx.CancellationToken);
                return;
            }

            // Discard?
            foreach (var rule in _options.ExceptionRules)
            {
                if (rule.ExceptionType.IsAssignableFrom(ex.GetType()) && rule.Action == FailureAction.Discard)
                {
                    _log.LogWarning(ex, "Discarding {Type} per policy", ctx.Envelope.MessageType);
                    return;
                }
            }

            // Default: rethrow → DLQ
            _log.LogError(ex, "Permanent error for {Type} after {Attempts} attempts",
                ctx.Envelope.MessageType, attempt);
            throw;
        }
    }

    private TimeSpan CalculateBackoff(int attempt)
    {
        var baseSeconds = _options.DelayedBackoffBaseSeconds;
        var maxSeconds = _options.DelayedBackoffMaxSeconds;
        var jitter = Random.Shared.NextDouble();
        var delay = Math.Min(maxSeconds, baseSeconds * Math.Pow(2, attempt) * (1 + jitter * 0.5));
        return TimeSpan.FromSeconds(delay);
    }
}
```

---

## AvtoBus/InMemoryDeferralSink.cs

```csharp
namespace AvtoBus;

public interface IDeferralSink
{
    ValueTask DeferAsync(Envelope envelope, TimeSpan delay, CancellationToken ct);
}

internal sealed class InMemoryDeferralSink : IDeferralSink
{
    public ValueTask DeferAsync(Envelope envelope, TimeSpan delay, CancellationToken ct)
    {
        // В InMemory-режиме просто задерживаем
        _ = Task.Delay(delay, ct).ContinueWith(_ =>
        {
            // В реальном приложении: enqueue обратно в транспорт
        }, ct);
        return default;
    }
}
```

---

## AvtoBus/IKeepAliveSink.cs

```csharp
namespace AvtoBus;

public interface IKeepAliveSink
{
    ValueTask KeepAliveAsync(Envelope envelope, CancellationToken ct);
}
```

---

## AvtoBus.Dashboard/AvtoBusDashboardExtensions.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Расширения для подключения дашборда.
/// </summary>
public static class DashboardExtensions
{
    public static IServiceCollection AddAvtoBusDashboard(
        this IServiceCollection services,
        Action<DashboardOptions>? configure = null)
    {
        var options = new DashboardOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        services.AddSingleton<DashboardService>();
        return services;
    }

    public static IEndpointRouteBuilder MapAvtoBusDashboard(
        this IEndpointRouteBuilder app,
        string pattern = "/bus")
    {
        var group = app.MapGroup(pattern);

        group.MapGet("/api/overview", async (DashboardService svc, CancellationToken ct) =>
        {
            return Results.Ok(await svc.GetOverviewAsync(ct));
        });

        group.MapGet("/api/queues", async (DashboardService svc, CancellationToken ct) =>
        {
            return Results.Ok(await svc.ListQueuesAsync(ct));
        });

        group.MapGet("/api/dlq/{queue}", async (
            string queue, [AsParameters] PagingRequest paging,
            DashboardService svc, CancellationToken ct) =>
        {
            return Results.Ok(await svc.ListDeadLettersAsync(queue, paging.Skip, paging.Take, ct));
        });

        group.MapPost("/api/dlq/{queue}/replay", async (
            string queue, ReplayRequest request,
            DashboardService svc, CancellationToken ct) =>
        {
            var count = await svc.ReplayDeadLettersAsync(queue, request, ct);
            return Results.Ok(new { Replayed = count });
        });

        group.MapGet("/api/sagas", async (
            [AsParameters] SagaQuery query,
            DashboardService svc, CancellationToken ct) =>
        {
            return Results.Ok(await svc.ListSagasAsync(query, ct));
        });

        group.MapGet("/api/sagas/{id:guid}", async (
            Guid id, DashboardService svc, CancellationToken ct) =>
        {
            var saga = await svc.GetSagaAsync(id, ct);
            return saga is not null ? Results.Ok(saga) : Results.NotFound();
        });

        return app;
    }
}

public sealed class DashboardOptions
{
    public string RoutePrefix { get; set; } = "/bus";
    public bool AllowDangerousOperationsInProduction { get; set; }
    public string PolicyName { get; set; } = "AvtoBusDashboard";
}

public sealed record PagingRequest(int Skip = 0, int Take = 50);
public sealed record ReplayRequest(int MaxParallelism = 10, string? Filter = null);
public sealed record SagaQuery(string? Type = null, string? Status = null, int Skip = 0, int Take = 50);
```

---

## AvtoBus.Dashboard/DashboardService.cs

```csharp
namespace AvtoBus;

public sealed class DashboardService
{
    private readonly ISagaStore _sagaStore;
    private readonly IOutboxStatus? _outboxStatus;

    public DashboardService(ISagaStore sagaStore, IOutboxStatus? outboxStatus = null)
    {
        _sagaStore = sagaStore;
        _outboxStatus = outboxStatus;
    }

    public async ValueTask<OverviewVm> GetOverviewAsync(CancellationToken ct)
    {
        var sagas = await _sagaStore.QueryAsync(take: 10000, ct: ct);
        return new OverviewVm(
            ActiveSagas: sagas.Count(s => s.Status == "Active"),
            CompletedSagas: sagas.Count(s => s.Status == "Completed"),
            OutboxPending: _outboxStatus?.PendingCount ?? 0);
    }

    public ValueTask<IReadOnlyList<QueueVm>> ListQueuesAsync(CancellationToken ct)
    {
        // В реальном приложении — из транспорта
        return ValueTask.FromResult<IReadOnlyList<QueueVm>>(Array.Empty<QueueVm>());
    }

    public async ValueTask<IReadOnlyList<SagaInstance>> ListSagasAsync(
        SagaQuery query, CancellationToken ct)
    {
        return await _sagaStore.QueryAsync(
            sagaType: query.Type is not null ? Type.GetType(query.Type) : null,
            status: query.Status,
            skip: query.Skip,
            take: query.Take,
            ct: ct);
    }

    public async ValueTask<SagaInstance?> GetSagaAsync(Guid id, CancellationToken ct)
    {
        return await _sagaStore.GetAsync(id, ct);
    }

    public ValueTask<int> ReplayDeadLettersAsync(
        string queue, ReplayRequest request, CancellationToken ct)
    {
        // В реальном приложении — читаем из DLQ, фильтруем, republish
        return ValueTask.FromResult(0);
    }

    public async ValueTask<IReadOnlyList<SagaInstance>> ListDeadLettersAsync(
        string queue, int skip, int take, CancellationToken ct)
    {
        return await _sagaStore.QueryAsync(take: take, ct: ct);
    }
}

public sealed record OverviewVm(int ActiveSagas, int CompletedSagas, int OutboxPending);
public sealed record QueueVm(string Name, int Messages, int Consumers, double Rate, int DeadLetterCount);
public sealed record DeadLetterVm(Guid Id, string MessageType, string Error);
public interface IOutboxStatus { int PendingCount { get; } }
```

---

## AvtoBus.Testing/AvtoBusTestHarness.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace AvtoBus.Testing;

/// <summary>
/// Тест-харнесс для AvtoBus: InMemory-транспорт + ловля сообщений + виртуальное время.
/// </summary>
public sealed class AvtoBusTestHarness : IAsyncDisposable
{
    public IBus Bus { get; }
    public FakeTimeProvider Clock { get; } = new();
    public TestTransport Transport { get; }
    public InMemorySagaStore Sagas { get; } = new();
    public IServiceProvider Services { get; }
    public CapturedMessages Captured { get; } = new();

    private readonly IServiceScope _scope;

    private AvtoBusTestHarness(Action<IServiceCollection, BusOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Trace));
        services.AddSingleton<TimeProvider>(Clock);

        var options = new BusOptions(services);
        configure?.Invoke(services, options);

        // InMemory + Test transport
        Transport = new TestTransport(Clock);
        services.AddSingleton<ITransport>(Transport);
        services.AddSingleton<ISagaStore>(Sagas);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
        });

        Services = provider;
        _scope = provider.CreateScope();
        Bus = provider.GetRequiredService<IBus>();
    }

    public static async ValueTask<AvtoBusTestHarness> CreateAsync(
        Action<IServiceCollection>? configureServices = null,
        Action<BusOptions>? configureBus = null)
    {
        var h = new AvtoBusTestHarness((services, options) =>
        {
            configureServices?.Invoke(services);
            configureBus?.Invoke(options);
        });
        return h;
    }

    /// <summary>
    /// Опубликовать сообщение и дождаться обработки.
    /// </summary>
    public async Task PublishAndWait<T>(T @event, TimeSpan? timeout = null) where T : class
    {
        await Bus.Publish(@event);
        await Transport.DrainAsync(timeout ?? TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Отправить команду и дождаться обработки.
    /// </summary>
    public async Task SendAndWait<T>(T command, TimeSpan? timeout = null) where T : class
    {
        await Bus.Send(command);
        await Transport.DrainAsync(timeout ?? TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Доступ ко всем отправленным сообщениям определённого типа.
    /// </summary>
    public IEnumerable<T> Published<T>() where T : class
        => Captured.Published.OfType<T>();

    /// <summary>
    /// Доступ ко всем обработанным сообщениям определённого типа.
    /// </summary>
    public IEnumerable<T> Consumed<T>() where T : class
        => Captured.Consumed.OfType<T>();

    /// <summary>
    /// Дождаться публикации сообщения.
    /// </summary>
    public async Task<T?> WaitForPublished<T>(TimeSpan? timeout = null) where T : class
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (DateTime.UtcNow < deadline)
        {
            var msg = Published<T>().FirstOrDefault();
            if (msg is not null) return msg;
            await Task.Delay(10);
        }
        return default;
    }

    /// <summary>
    /// Продвинуть виртуальное время.
    /// </summary>
    public ValueTask AdvanceTime(TimeSpan delta)
    {
        Clock.Advance(delta);
        return Transport.DrainAsync(TimeSpan.FromSeconds(1));
    }

    public ValueTask DisposeAsync()
    {
        if (_scope is IAsyncDisposable d)
            return d.DisposeAsync();
        _scope.Dispose();
        return default;
    }
}

/// <summary>
/// Перехват всех сообщений шины.
/// </summary>
public sealed class CapturedMessages
{
    public List<object> Published { get; } = new();
    public List<object> Consumed { get; } = new();
    public List<Envelope> Envelopes { get; } = new();
    public List<(string Queue, Envelope Envelope, string Error)> DeadLettered { get; } = new();
    public List<SagaInstance> SagasStarted { get; } = new();
}
```

---

## AvtoBus.Testing/TestTransport.cs

```csharp
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AvtoBus.Testing;

/// <summary>
/// Тестовый транспорт: InMemory + перехват всех сообщений.
/// </summary>
public sealed class TestTransport : ITransport
{
    public string Name => "test";
    public CapturedMessages Captured { get; }

    private readonly InMemoryTransport _inner;
    private readonly FakeTimeProvider _clock;

    public TestTransport(FakeTimeProvider clock)
    {
        _clock = clock;
        _inner = new InMemoryTransport(clock);
    }

    public TestTransport(FakeTimeProvider clock, CapturedMessages captured) : this(clock)
    {
        Captured = captured;
    }

    public ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct)
    {
        Captured?.Published.Add(JsonSerializer.Deserialize<object>(envelope.Body)!);
        Captured?.Envelopes.Add(envelope);
        return _inner.SendAsync(envelope, dest, ct);
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var msg in _inner.ReceiveAsync(subscription, ct))
        {
            var payload = JsonSerializer.Deserialize<object>(msg.Envelope.Body);
            if (payload is not null)
                Captured?.Consumed.Add(payload);

            yield return new TransportMessage(msg.Envelope, new CapturedAck(msg.Ack, Captured));
        }
    }

    public async Task DrainAsync(TimeSpan timeout)
    {
        await Task.Delay(timeout);
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public void Dispose() => _inner.Dispose();
}

internal sealed class CapturedAck(IAckContext inner, CapturedMessages? captured) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default) => inner.AckAsync(ct);

    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        if (!requeue && captured is not null)
            captured.DeadLettered.Add(("", new Envelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = "unknown",
                Body = ReadOnlyMemory<byte>.Empty,
            }, "test"));
        return inner.NackAsync(requeue, ct);
    }

    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
        => inner.DeferAsync(delay, ct);
}
```
