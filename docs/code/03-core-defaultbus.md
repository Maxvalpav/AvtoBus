# AvtoBus.Core — DefaultBus и реализация IBus

> **Code sketch / unverified.** Lifetime, UoW и reply-routing требуют сведения с `14-reliability-glue.md`. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Core/DefaultBus.cs

```csharp
using AvtoBus.Dispatching;
using AvtoBus.Pipeline;
using Microsoft.Extensions.Logging;

namespace AvtoBus;

/// <summary>
/// Стандартная реализация IBus.
/// Маршрутизирует сообщения, управляет конвертами, делегирует транспортам.
/// </summary>
internal sealed class DefaultBus : IBus
{
    private readonly IRouter _router;
    private readonly ITransportSelector _transports;
    private readonly ISerializer _serializer;
    private readonly ITypeResolver _typeResolver;
    private readonly IBusContextAccessor _accessor;
    private readonly IBusState _state;
    private readonly IOutbox? _outbox;
    private readonly TimeProvider _clock;
    private readonly ILogger<DefaultBus> _log;

    public DefaultBus(
        IRouter router,
        ITransportSelector transports,
        ISerializer serializer,
        ITypeResolver typeResolver,
        IBusContextAccessor accessor,
        IBusState state,
        TimeProvider clock,
        ILogger<DefaultBus> log,
        IOutbox? outbox = null)
    {
        _router = router;
        _transports = transports;
        _serializer = serializer;
        _typeResolver = typeResolver;
        _accessor = accessor;
        _state = state;
        _outbox = outbox;
        _clock = clock;
        _log = log;
    }

    // ── Publish ──

    public ValueTask Publish<T>(T @event, PublishOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        return PublishInternalAsync(@event, typeof(T), options, ct);
    }

    public ValueTask Publish(object @event, Type eventType, PublishOptions? options = null,
        CancellationToken ct = default)
    {
        return PublishInternalAsync(@event, eventType, options, ct);
    }

    private async ValueTask PublishInternalAsync(object @event, Type eventType,
        PublishOptions? options, CancellationToken ct)
    {
        var route = _router.Route(eventType, isCommand: false);
        var body = _serializer.Serialize(@event);
        var envelope = BuildEnvelope(eventType, body, options);

        _log.LogDebug("Publishing {Type} ({MessageId})", envelope.MessageType, envelope.MessageId);

        // Publisher-спан для трейса
        using var activity = BusTracing.StartPublish(envelope.MessageType, envelope);
        var sw = System.Diagnostics.Stopwatch.GetTimestamp();
        var status = "ok";

        try
        {
            // Outbox: если есть активный UoW — записать, не отправлять
            if (_outbox is not null && _accessor.Current?.HasUnitOfWork == true)
            {
                await _outbox.EnqueueAsync(envelope, route, ct);
                return;
            }

            var transport = _transports.For(route.Transport);
            await transport.SendAsync(envelope, route.Destination, ct);
        }
        catch (Exception ex)
        {
            status = "error";
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            throw;
        }
        finally
        {
            var ms = System.Diagnostics.Stopwatch.GetElapsedTime(sw).TotalMilliseconds;
            BusMetrics.PublishCount.Add(1, new TagList { { "type", envelope.MessageType }, { "status", status } });
            BusMetrics.PublishDuration.Record(ms, new TagList { { "type", envelope.MessageType } });
        }
    }

    // ── Send ──

    public ValueTask Send<T>(T command, SendOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        return SendInternalAsync(command, typeof(T), options, ct);
    }

    public async ValueTask SendInternalAsync(object command, Type commandType,
        SendOptions? options, CancellationToken ct)
    {
        var route = _router.Route(commandType, isCommand: true);
        var body = _serializer.Serialize(command);
        var envelope = BuildEnvelope(commandType, body, options);

        _log.LogDebug("Sending {Type} ({MessageId})", envelope.MessageType, envelope.MessageId);

        if (_outbox is not null && _accessor.Current?.HasUnitOfWork == true)
        {
            await _outbox.EnqueueAsync(envelope, route, ct);
            return;
        }

        var transport = _transports.For(route.Transport);
        await transport.SendAsync(envelope, route.Destination, ct);
    }

    // ── Request/Response ──

    public async ValueTask<TReply> Request<T, TReply>(T request, TimeSpan? timeout = null,
        CancellationToken ct = default) where T : class where TReply : class
    {
        var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);
        var replyTo = _state.RegisterReply(tcs, timeout ?? TimeSpan.FromSeconds(30));

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeout is { } t)
            cts.CancelAfter(t);

        try
        {
            await Send(request, new SendOptions
            {
                Headers = new Dictionary<string, string>
                {
                    ["avtobus.reply-to"] = replyTo
                }
            }, cts.Token);

            return await tcs.Task.WaitAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Request/Response timed out after {timeout ?? TimeSpan.FromSeconds(30)}");
        }
    }

    // ── Schedule ──

    public ValueTask<Guid> Schedule<T>(T message, DateTimeOffset at,
        CancellationToken ct = default) where T : class
    {
        var delay = at - _clock.GetUtcNow();
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
        return Schedule(message, delay, ct);
    }

    public async ValueTask<Guid> Schedule<T>(T message, TimeSpan delay,
        CancellationToken ct = default) where T : class
    {
        var token = Guid.NewGuid();

        _log.LogDebug("Scheduling {Type} in {Delay}", typeof(T).Name, delay);

        await Publish(message, new PublishOptions
        {
            Delay = delay,
            Headers = new Dictionary<string, string>
            {
                ["avtobus.schedule-token"] = token.ToString()
            }
        }, ct);

        _state.TrackScheduled(token);
        return token;
    }

    public ValueTask CancelScheduled(Guid token, CancellationToken ct = default)
    {
        _state.CancelScheduled(token);
        return ValueTask.CompletedTask;
    }

    // ── EnqueueLocal ──

    public ValueTask EnqueueLocal<T>(T message, CancellationToken ct = default) where T : class
    {
        var route = _router.Route(typeof(T), isCommand: false);
        var body = _serializer.Serialize(message);
        var envelope = BuildEnvelope(typeof(T), body, null);

        // Локальная доставка: сразу в pipeline
        var local = _transports.For("local");
        return local.SendAsync(envelope, new TransportDestination(route.Destination.Address, DestinationKind.Queue), ct);
    }

    // ── Вспомогательные ──

    private Envelope BuildEnvelope(Type clrType, ReadOnlyMemory<byte> body, PublishOptions? opts)
    {
        var current = _accessor.Current;
        var now = _clock.GetUtcNow();
        var messageId = Guid.NewGuid();

        var headers = new Dictionary<string, string>(opts?.Headers ?? new());

        // Автоматические заголовки
        headers["avtobus.message-type"] = _typeResolver.GetName(clrType);
        headers["avtobus.sent-at"] = now.ToString("O");
        headers["avtobus.version"] = "1";

        if (opts?.Consumer is { } consumer)
            headers["consumer"] = consumer;

        return new Envelope
        {
            MessageId = messageId,
            CorrelationId = current?.Envelope.CorrelationId ?? messageId,
            CausationId = current?.Envelope.MessageId,
            MessageType = _typeResolver.GetName(clrType),
            Body = body,
            SentAt = now,
            DeliverAt = opts?.Delay is { } d ? now.Add(d) : null,
            TimeToLive = opts?.Ttl,
            PartitionKey = opts?.PartitionKey,
            TenantId = opts?.TenantId ?? current?.Envelope.TenantId,
            Priority = opts?.Priority ?? 4,
            TraceParent = System.Diagnostics.Activity.Current?.Id,
            Headers = headers.ToFrozenDictionary(),
        };
    }
}
```

---

## AvtoBus.Core/BusHost.cs

```csharp
using AvtoBus.Dispatching;
using AvtoBus.Pipeline;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus;

/// <summary>
/// BackgroundService: принимает сообщения от транспортов и прогоняет через пайплайн.
/// </summary>
internal sealed class BusHost : BackgroundService
{
    private readonly ITransportSelector _transports;
    private readonly DispatcherRegistry _dispatchers;
    private readonly ISerializer _serializer;
    private readonly ISubscriptionCatalog _catalog;
    private readonly BusPipelineBuilder _pipelineBuilder;
    private readonly IBusContextAccessor _accessor;
    private readonly RecoverabilityOptions _recoverability;
    private readonly TimeProvider _clock;
    private readonly ILogger<BusHost> _log;

    public BusHost(
        ITransportSelector transports,
        DispatcherRegistry dispatchers,
        ISerializer serializer,
        ISubscriptionCatalog catalog,
        BusPipelineBuilder pipelineBuilder,
        IBusContextAccessor accessor,
        RecoverabilityOptions recoverability,
        TimeProvider clock,
        ILogger<BusHost> log)
    {
        _transports = transports;
        _dispatchers = dispatchers;
        _serializer = serializer;
        _catalog = catalog;
        _pipelineBuilder = pipelineBuilder;
        _accessor = accessor;
        _recoverability = recoverability;
        _clock = clock;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("AvtoBus Host starting. Subscriptions: {Count}", _catalog.Subscriptions.Count);

        var pipeline = _pipelineBuilder.Build(async ctx =>
        {
            // Терминальный хендлер — вызов dispatch
            var invoker = ctx.Services.GetRequiredService<HandlerInvokerMiddleware>();
            await invoker.InvokeAsync(ctx, _ => default);
        });

        var tasks = new List<Task>();
        foreach (var sub in _catalog.Subscriptions)
        {
            tasks.Add(RunSubscription(sub, pipeline, stoppingToken));
        }

        await Task.WhenAll(tasks);
    }

    private async Task RunSubscription(
        SubscriptionEntry sub,
        BusDelegate pipeline,
        CancellationToken ct)
    {
        var transport = _transports.For(sub.TransportName);

        _log.LogInformation("Starting consumer: {Consumer} on {Transport}/{Destination}",
            sub.ConsumerId, sub.TransportName, sub.Destination);

        try
        {
            await foreach (var transportMessage in transport.ReceiveAsync(
                new TransportSubscription(sub.Destination, sub.Topics, sub.Prefetch, sub.ConsumerId), ct))
            {
                _ = ProcessMessage(transportMessage, pipeline, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _log.LogInformation("Consumer {Consumer} stopped.", sub.ConsumerId);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Fatal error in consumer {Consumer}", sub.ConsumerId);
        }
    }

    private async Task ProcessMessage(
        TransportMessage transportMsg,
        BusDelegate pipeline,
        CancellationToken ct)
    {
        var envelope = transportMsg.Envelope;
        var sw = System.Diagnostics.Stopwatch.GetTimestamp();

        _log.LogTrace("Received {Type} (attempt {Attempt}, id={Id})",
            envelope.MessageType, envelope.DeliveryAttempt, envelope.MessageId);

        // Проверка TTL
        if (envelope.IsExpired(_clock.GetUtcNow()))
        {
            _log.LogWarning("Message expired, dead-lettering: {Type} {Id}", envelope.MessageType, envelope.MessageId);
            await transportMsg.Ack.NackAsync(requeue: false, ct);
            BusMetrics.DeadLetteredCount.Add(1);
            return;
        }

        // Проверка дедлайна
        if (envelope.IsOverdue(_clock.GetUtcNow()))
        {
            _log.LogWarning("Message overdue, dead-lettering: {Type} {Id}", envelope.MessageType, envelope.MessageId);
            await transportMsg.Ack.NackAsync(requeue: false, ct);
            return;
        }

        // Найти диспетчер
        if (!_dispatchers.TryGet(envelope.MessageType, out var dispatcher))
        {
            _log.LogWarning("No handler for {Type}, moving to DLQ", envelope.MessageType);
            await transportMsg.Ack.NackAsync(requeue: false, ct);
            BusMetrics.DeadLetteredCount.Add(1);
            return;
        }

        // Десериализовать
        object message;
        try
        {
            message = _serializer.Deserialize(envelope.Body, dispatcher.ClrType);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Deserialization failed for {Type}", envelope.MessageType);
            await transportMsg.Ack.NackAsync(requeue: false, ct);
            return;
        }

        // Создать scope + context
        ConsumeContext? ctx = null;
        var attempt = envelope.DeliveryAttempt;

        var maxAttempts = _recoverability.ImmediateRetries + _recoverability.DelayedRetries + 1;

        while (attempt < maxAttempts)
        {
            try
            {
                ctx = new ConsumeContext
                {
                    Envelope = envelope.WithAttempt(attempt),
                    Message = message,
                    Services = null!,  // подставится в ScopeMiddleware
                    CancellationToken = ct,
                    StartedAt = _clock.GetUtcNow(),
                };

                _accessor.Current = ctx;

                using var activity = BusTracing.StartConsume(envelope);

                var msgSw = System.Diagnostics.Stopwatch.GetTimestamp();
                await pipeline(ctx);
                var msgMs = System.Diagnostics.Stopwatch.GetElapsedTime(msgSw).TotalMilliseconds;

                var criticalMs = (DateTimeOffset.UtcNow - envelope.SentAt).TotalMilliseconds;
                BusMetrics.RecordConsume(envelope.MessageType, msgMs);
                BusMetrics.RecordCriticalTime(criticalMs);

                // Ack
                await transportMsg.Ack.AckAsync(ct);
                return;
            }
            catch (DeadLetterException ex)
            {
                _log.LogWarning(ex, "Message dead-lettered by handler: {Type}", envelope.MessageType);
                await transportMsg.Ack.NackAsync(requeue: false, ct);
                BusMetrics.DeadLetteredCount.Add(1);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsRetryable(ex) && attempt < maxAttempts - 1)
            {
                attempt++;
                BusMetrics.RetryCount.Add(1);
                _log.LogWarning(ex, "Retryable error (attempt {Attempt}/{Max}) for {Type}",
                    attempt, maxAttempts, envelope.MessageType);

                if (attempt <= _recoverability.ImmediateRetries)
                    continue;

                // Delayed retry: ждём в retry-очереди (через Defer или backoff)
                var delay = CalculateBackoff(attempt - _recoverability.ImmediateRetries);
                await transportMsg.Ack.DeferAsync(delay, ct);
                return;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Permanent error for {Type}, moving to DLQ", envelope.MessageType);
                await transportMsg.Ack.NackAsync(requeue: false, ct);
                BusMetrics.DeadLetteredCount.Add(1);
                BusMetrics.ConsumeErrorCount.Add(1);
                return;
            }
        }

        // Все попытки исчерпаны
        _log.LogError("All retries exhausted for {Type}, moving to DLQ", envelope.MessageType);
        await transportMsg.Ack.NackAsync(requeue: false, ct);
        BusMetrics.DeadLetteredCount.Add(1);
    }

    private bool IsRetryable(Exception ex)
    {
        // Транзиентные исключения → ретраить
        if (ex is TimeoutException or HttpRequestException or IOException)
            return true;

        // Если есть атрибут [Transient] или маркер
        return ex.Data.Contains("AvtoBus.Transient");
    }

    private TimeSpan CalculateBackoff(int attempt)
    {
        var baseSeconds = _recoverability.DelayedBackoffBaseSeconds;
        var maxSeconds = _recoverability.DelayedBackoffMaxSeconds;

        // Decorrelated jitter: min(cap, random(base, prev*3))
        var delay = Math.Min(maxSeconds, baseSeconds * Math.Pow(3, attempt - 1));
        var jitter = Random.Shared.NextDouble() * delay;
        return TimeSpan.FromSeconds(jitter);
    }
}
```

---

## AvtoBus.Core/DefaultBus.CreateAsync.cs

```csharp
using System.Reflection;
using AvtoBus.Dispatching;
using AvtoBus.Pipeline;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus;

/// <summary>
/// Фабрика: подключает всё в DI, создаёт диспетчеры, строит pipeline.
/// </summary>
internal static class DefaultBusFactory
{
    public static IBus Create(BusOptions options)
    {
        var services = options.Services;

        // ── Базовые сервисы ──
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IBusContextAccessor, AsyncLocalBusContextAccessor>();
        services.TryAddSingleton<IBusState, InMemoryBusState>();
        services.TryAddSingleton<IDeferralSink, InMemoryDeferralSink>();
        services.TryAddSingleton<ISerializer, DefaultJsonSerializer>();
        services.TryAddSingleton<ITypeResolver, TypeAliasResolver>();
        services.TryAddSingleton<IRouter, ConventionRouter>();
        services.TryAddSingleton<ITransportSelector, InMemoryTransportSelector>();
        services.TryAddSingleton<ISubscriptionCatalog, ReflectionSubscriptionCatalog>();

        // ── Собираем диспетчеры из assemblies и конкретных типов ──
        services.AddSingleton(sp =>
        {
            var dispatchers = new List<IMessageDispatcher>();
            dispatchers.AddRange(ReflectionDispatcherBuilder.BuildFromAssemblies(
                options.ConsumerAssemblies, sp));
            dispatchers.AddRange(ReflectionDispatcherBuilder.BuildFromTypes(
                options.ConsumerTypes, sp));
            return new DispatcherRegistry(dispatchers);
        });

        // ── BusOptions для других сервисов ──
        services.AddSingleton(options);

        // ── DefaultBus как IBus ──
        services.AddSingleton<IBus>(sp =>
        {
            var bus = new DefaultBus(
                sp.GetRequiredService<IRouter>(),
                sp.GetRequiredService<ITransportSelector>(),
                sp.GetRequiredService<ISerializer>(),
                sp.GetRequiredService<ITypeResolver>(),
                sp.GetRequiredService<IBusContextAccessor>(),
                sp.GetRequiredService<IBusState>(),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<ILogger<DefaultBus>>(),
                sp.GetService<IOutbox>());

            return bus;
        });

        // ── Pipeline builder ──
        services.AddSingleton(sp =>
        {
            var builder = new BusPipelineBuilder();
            options.Pipeline.Configure(builder);

            // Дефолтный пайплайн, если пустой
            if (builder.Count == 0)
            {
                ApplyDefaultPipeline(builder);
            }

            return builder;
        });

        // ── Терминальный middleware ──
        services.AddSingleton<HandlerInvokerMiddleware>();

        // ── Hosted Service: BusHost ──
        services.AddHostedService<BusHost>();

        return sp =>
        {
            // Нет — мы возвращаем IBus через DI, а не через фабрику
            // (оставлено для обратной совместимости, реально используем DI)
        };
    }

    private static void ApplyDefaultPipeline(BusPipelineBuilder builder)
    {
        builder.Use<TelemetryMiddleware>();
        builder.Use<ScopeMiddleware>();
        builder.Use<TenantMiddleware>();
        builder.Use<InboxDedupMiddleware>();
        builder.Use<RecoverabilityMiddleware>();
    }
}
```

---

## AvtoBus.Core/BusState.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus;

/// <summary>
/// Внутреннее состояние шины: reply-токены, scheduled-сессии.
/// </summary>
public interface IBusState
{
    string RegisterReply<T>(TaskCompletionSource<T> tcs, TimeSpan timeout) where T : class;
    void TrackScheduled(Guid token);
    void CancelScheduled(Guid token);
    bool IsScheduledCancelled(Guid token);
}

internal sealed class InMemoryBusState : IBusState, IDisposable
{
    private readonly ConcurrentDictionary<string, (object Tcs, Timer Timer)> _replies = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _scheduled = new();
    private int _replyCounter;

    public string RegisterReply<T>(TaskCompletionSource<T> tcs, TimeSpan timeout) where T : class
    {
        var replyTo = $"avtobus-reply-{Interlocked.Increment(ref _replyCounter)}";
        var cts = new CancellationTokenSource(timeout);
        var timer = new Timer(_ =>
        {
            tcs.TrySetException(new TimeoutException());
            cts.Dispose();
            _replies.TryRemove(replyTo, out _);
        }, null, timeout, Timeout.InfiniteTimeSpan);

        _replies[replyTo] = (tcs, timer);
        return replyTo;
    }

    public void CompleteReply<T>(string replyTo, T reply) where T : class
    {
        if (_replies.TryRemove(replyTo, out var entry))
        {
            entry.Timer.Dispose();
            ((TaskCompletionSource<T>)entry.Tcs).TrySetResult(reply);
        }
    }

    public void TrackScheduled(Guid token)
    {
        var cts = new CancellationTokenSource();
        _scheduled[token] = cts;
    }

    public void CancelScheduled(Guid token)
    {
        if (_scheduled.TryRemove(token, out var cts))
            cts.Cancel();
    }

    public bool IsScheduledCancelled(Guid token)
    {
        return _scheduled.TryGetValue(token, out var cts) && cts.IsCancellationRequested;
    }

    public void Dispose()
    {
        foreach (var entry in _replies.Values)
        {
            entry.Timer.Dispose();
        }
        foreach (var cts in _scheduled.Values)
        {
            cts.Dispose();
        }
    }
}
```
