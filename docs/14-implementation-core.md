# 🔧 Реализация: Core (Envelope, IBus, Pipeline)

> **Design draft.** Код ниже является эскизом целевой реализации и не прошёл совместную компиляцию. Приоритет у контрактов из `03-core-api.md` до появления реального `src/`.

## 1. Envelope и системные типы

```csharp
// AvtoBus.Core/Envelope.cs
using System.Collections.Frozen;

namespace AvtoBus;

public sealed record Envelope
{
    public required Guid MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public required string MessageType { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public string ContentType { get; init; } = "application/json";
    public string ContentEncoding { get; init; } = "identity";
    public DateTimeOffset SentAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DeliverAt { get; init; }
    public DateTimeOffset? Deadline { get; init; }
    public TimeSpan? TimeToLive { get; init; }
    public string? PartitionKey { get; init; }
    public string? TenantId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Source { get; init; }
    public int DeliveryAttempt { get; init; }
    public byte Priority { get; init; } = 4;
    public string? TraceParent { get; init; }
    public string? TraceState { get; init; }
    public IReadOnlyDictionary<string, string> Headers { get; init; }
        = FrozenDictionary<string, string>.Empty;

    public Envelope WithAttempt(int attempt) => this with { DeliveryAttempt = attempt };
    public Envelope WithHeader(string k, string v)
    {
        var copy = new Dictionary<string, string>(Headers) { [k] = v };
        return this with { Headers = copy.ToFrozenDictionary() };
    }
}

public readonly record struct TransportDestination(string Address, DestinationKind Kind);
public enum DestinationKind { Queue, Topic, Reply }

public sealed record TransportMessage(
    Envelope Envelope,
    IAckContext Ack);

public interface IAckContext
{
    ValueTask AckAsync(CancellationToken ct = default);
    ValueTask NackAsync(bool requeue = false, CancellationToken ct = default);
    ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default);
}
```

## 2. Публичный контракт IBus

```csharp
// AvtoBus.Core/IBus.cs
namespace AvtoBus;

public interface IBus
{
    ValueTask Publish<T>(T @event, PublishOptions? options = null,
        CancellationToken ct = default) where T : class;

    ValueTask Send<T>(T command, SendOptions? options = null,
        CancellationToken ct = default) where T : class;

    ValueTask<TReply> Request<T, TReply>(T request, TimeSpan? timeout = null,
        CancellationToken ct = default) where T : class where TReply : class;

    ValueTask<Guid> Schedule<T>(T message, DateTimeOffset at,
        CancellationToken ct = default) where T : class;

    ValueTask CancelScheduled(Guid token, CancellationToken ct = default);
}

public sealed record PublishOptions
{
    public string? PartitionKey { get; init; }
    public TimeSpan? Delay { get; init; }
    public TimeSpan? Ttl { get; init; }
    public byte? Priority { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public string? TenantId { get; init; }
    public bool RequestReceipt { get; init; }
    public bool Unique { get; init; }
    public string? UniqueKey { get; init; }
    public TimeSpan? UniqueWindow { get; init; }
}

public sealed record SendOptions : PublishOptions;
```

## 3. ConsumeContext

```csharp
// AvtoBus.Core/ConsumeContext.cs
namespace AvtoBus;

public class ConsumeContext
{
    public required Envelope Envelope { get; init; }
    public required object Message { get; init; }
    public required IServiceProvider Services { get; init; }
    public required CancellationToken CancellationToken { get; init; }
    public int Attempt => Envelope.DeliveryAttempt;
    public IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();

    internal List<OutgoingMessage> Outgoing { get; } = new();

    public ValueTask PublishAsync<T>(T @event, PublishOptions? o = null) where T : class
    {
        Outgoing.Add(new OutgoingMessage(@event, OutgoingKind.Publish, o));
        return default;
    }

    public ValueTask SendAsync<T>(T command, SendOptions? o = null) where T : class
    {
        Outgoing.Add(new OutgoingMessage(command, OutgoingKind.Send, o));
        return default;
    }

    public ValueTask RespondAsync<T>(T reply) where T : class
    {
        if (Envelope.ReplyTo is null)
            throw new InvalidOperationException("Message has no ReplyTo — cannot respond.");
        Outgoing.Add(new OutgoingMessage(reply, OutgoingKind.Reply, null));
        return default;
    }

    public ValueTask DeferAsync(TimeSpan delay)
        => Services.GetRequiredService<IDeferralSink>().DeferAsync(Envelope, delay, CancellationToken);
}

public sealed class ConsumeContext<T> : ConsumeContext where T : class
{
    public new required T Message { get; init; }
}

internal enum OutgoingKind { Publish, Send, Reply }
internal sealed record OutgoingMessage(object Payload, OutgoingKind Kind, PublishOptions? Options);
```

## 4. Middleware-пайплайн (компилируемый)

```csharp
// AvtoBus.Core/Pipeline/BusPipeline.cs
using System.Runtime.CompilerServices;

namespace AvtoBus.Pipeline;

public delegate ValueTask BusDelegate(ConsumeContext context);

public interface IBusMiddleware
{
    ValueTask InvokeAsync(ConsumeContext context, BusDelegate next);
}

public sealed class BusPipelineBuilder
{
    private readonly List<Func<BusDelegate, BusDelegate>> _components = new();

    public BusPipelineBuilder Use<TMiddleware>() where TMiddleware : IBusMiddleware
    {
        _components.Add(next => async ctx =>
        {
            var mw = ctx.Services.GetRequiredService<TMiddleware>();
            await mw.InvokeAsync(ctx, next);
        });
        return this;
    }

    public BusPipelineBuilder Use(Func<ConsumeContext, BusDelegate, ValueTask> inline)
    {
        _components.Add(next => ctx => inline(ctx, next));
        return this;
    }

    public BusPipelineBuilder UseWhen(Predicate<ConsumeContext> pred,
        Action<BusPipelineBuilder> branch)
    {
        var b = new BusPipelineBuilder();
        branch(b);
        var branchDelegate = b.Build(_ => default);
        _components.Add(next => async ctx =>
        {
            if (pred(ctx)) await branchDelegate(ctx);
            await next(ctx);
        });
        return this;
    }

    public BusDelegate Build(BusDelegate terminal)
    {
        var app = terminal;
        for (var i = _components.Count - 1; i >= 0; i--)
            app = _components[i](app);
        return app;
    }
}
```

## 5. Диспетчер сообщений (генерируемый интерфейс)

```csharp
// AvtoBus.Core/Dispatching/IMessageDispatcher.cs
namespace AvtoBus.Dispatching;

public interface IMessageDispatcher
{
    string MessageType { get; }
    Type ClrType { get; }
    ValueTask DispatchAsync(ConsumeContext context);
}

// Регистр диспетчеров — заполняется Source Generator-ом при старте
public sealed class DispatcherRegistry
{
    private readonly FrozenDictionary<string, IMessageDispatcher> _byType;
    private readonly FrozenDictionary<Type, IMessageDispatcher> _byClr;

    public DispatcherRegistry(IEnumerable<IMessageDispatcher> dispatchers)
    {
        var list = dispatchers.ToArray();
        _byType = list.ToFrozenDictionary(d => d.MessageType);
        _byClr  = list.ToFrozenDictionary(d => d.ClrType);
    }

    public bool TryGet(string messageType, out IMessageDispatcher d) => _byType.TryGetValue(messageType, out d!);
    public bool TryGet(Type clr, out IMessageDispatcher d) => _byClr.TryGetValue(clr, out d!);
}
```

## 6. Реализация IBus (`DefaultBus`)

```csharp
// AvtoBus.Core/DefaultBus.cs
namespace AvtoBus;

internal sealed class DefaultBus : IBus
{
    private readonly IRouter _router;
    private readonly ITransportSelector _transports;
    private readonly ISerializer _serializer;
    private readonly IOutbox? _outbox;
    private readonly IBusContextAccessor _accessor;
    private readonly TimeProvider _clock;

    public DefaultBus(IRouter router, ITransportSelector transports, ISerializer serializer,
        IBusContextAccessor accessor, TimeProvider clock, IOutbox? outbox = null)
    {
        _router = router; _transports = transports; _serializer = serializer;
        _accessor = accessor; _clock = clock; _outbox = outbox;
    }

    public ValueTask Publish<T>(T e, PublishOptions? o = null, CancellationToken ct = default) where T : class
        => DispatchAsync(e, isCommand: false, o, ct);

    public ValueTask Send<T>(T c, SendOptions? o = null, CancellationToken ct = default) where T : class
        => DispatchAsync(c, isCommand: true, o, ct);

    private async ValueTask DispatchAsync<T>(T msg, bool isCommand, PublishOptions? o, CancellationToken ct) where T : class
    {
        var route = _router.Route(typeof(T), isCommand);
        var body  = _serializer.Serialize(msg);
        var envelope = BuildEnvelope(typeof(T), body, o);

        // Если есть outbox и мы внутри UnitOfWork — записываем в outbox, не в брокер
        if (_outbox is not null && _accessor.Current?.HasUnitOfWork == true)
        {
            await _outbox.EnqueueAsync(envelope, route, ct);
            return;
        }

        var transport = _transports.For(route.Transport);
        await transport.SendAsync(envelope, route.Destination, ct);
    }

    private Envelope BuildEnvelope(Type clrType, ReadOnlyMemory<byte> body, PublishOptions? o)
    {
        var current = _accessor.Current;
        var messageId = Guid.CreateVersion7();
        return new Envelope
        {
            MessageId = messageId,
            CorrelationId = current?.Envelope.CorrelationId ?? messageId,
            CausationId   = current?.Envelope.MessageId,
            MessageType   = TypeAliases.Get(clrType),
            Body          = body,
            SentAt        = _clock.GetUtcNow(),
            DeliverAt     = o?.Delay is { } d ? _clock.GetUtcNow().Add(d) : null,
            TimeToLive    = o?.Ttl,
            PartitionKey  = o?.PartitionKey,
            TenantId      = o?.TenantId ?? current?.Envelope.TenantId,
            Priority      = o?.Priority ?? 4,
            TraceParent   = System.Diagnostics.Activity.Current?.Id,
        };
    }

    public async ValueTask<TReply> Request<T, TReply>(T req, TimeSpan? timeout = null, CancellationToken ct = default)
        where T : class where TReply : class
    {
        var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);
        var replyTo = ReplyRegistry.Register(tcs, timeout ?? TimeSpan.FromSeconds(30));
        await Send(req, new SendOptions { Headers = new() { ["reply-to"] = replyTo } }, ct);
        return await tcs.Task.WaitAsync(ct);
    }

    public async ValueTask<Guid> Schedule<T>(T msg, DateTimeOffset at, CancellationToken ct = default) where T : class
    {
        var delay = at - _clock.GetUtcNow();
        var token = Guid.CreateVersion7();
        await Publish(msg, new PublishOptions { Delay = delay, Headers = new() { ["schedule-token"] = token.ToString() } }, ct);
        return token;
    }

    public ValueTask CancelScheduled(Guid token, CancellationToken ct = default)
        => _transports.Default.CancelScheduledAsync(token, ct);
}
```

## 7. Стандартные middleware ядра

```csharp
// AvtoBus.Core/Middleware/TelemetryMiddleware.cs
using System.Diagnostics;

public sealed class TelemetryMiddleware : IBusMiddleware
{
    internal static readonly ActivitySource Source = new("AvtoBus");
    private static readonly Meter Meter = new("AvtoBus");
    private static readonly Histogram<double> ConsumeDuration =
        Meter.CreateHistogram<double>("avtobus.consume.duration", "ms");
    private static readonly Counter<long> ConsumeCount =
        Meter.CreateCounter<long>("avtobus.consume.count");

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        using var activity = Source.StartActivity(
            $"consume {ctx.Envelope.MessageType}",
            ActivityKind.Consumer,
            parentId: ctx.Envelope.TraceParent);

        activity?.SetTag("messaging.system", "avtobus");
        activity?.SetTag("messaging.message.id", ctx.Envelope.MessageId);
        activity?.SetTag("messaging.destination.name", ctx.Envelope.MessageType);
        activity?.SetTag("avtobus.attempt", ctx.Attempt);

        var sw = Stopwatch.GetTimestamp();
        var status = "ok";
        try
        {
            await next(ctx);
        }
        catch (Exception ex)
        {
            status = "error";
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
        finally
        {
            var ms = Stopwatch.GetElapsedTime(sw).TotalMilliseconds;
            var tags = new TagList
            {
                { "type", ctx.Envelope.MessageType },
                { "status", status }
            };
            ConsumeDuration.Record(ms, tags);
            ConsumeCount.Add(1, tags);
        }
    }
}

// AvtoBus.Core/Middleware/ScopeMiddleware.cs
public sealed class ScopeMiddleware : IBusMiddleware
{
    private readonly IServiceScopeFactory _scopes;
    public ScopeMiddleware(IServiceScopeFactory scopes) => _scopes = scopes;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var scoped = (ConsumeContext)ctx with { }; // копия record-а
        typeof(ConsumeContext).GetProperty(nameof(ConsumeContext.Services))!
            .SetValue(scoped, scope.ServiceProvider);
        // (реальная реализация — конструктор ctx с injected scope)
        var accessor = scope.ServiceProvider.GetRequiredService<IBusContextAccessor>();
        accessor.Current = scoped;
        await next(scoped);
    }
}
```

## 8. Регистрация в DI

```csharp
// AvtoBus.Core/DependencyInjection/ServiceCollectionExtensions.cs
public static class AvtoBusServiceCollectionExtensions
{
    public static IServiceCollection AddAvtoBus(this IServiceCollection s, Action<BusOptions> configure)
    {
        var options = new BusOptions(s);
        configure(options);

        s.AddSingleton(TimeProvider.System);
        s.AddSingleton<DispatcherRegistry>();
        s.AddSingleton<IRouter, ConventionRouter>();
        s.AddSingleton<ITransportSelector, TransportSelector>();
        s.AddSingleton<ISerializer, SystemTextJsonSerializer>();
        s.AddSingleton<IBusContextAccessor, AsyncLocalBusContextAccessor>();
        s.AddSingleton<IBus, DefaultBus>();
        s.AddHostedService<BusHost>();

        options.ApplyPipelineDefaults();
        return s;
    }
}

public sealed class BusOptions
{
    public IServiceCollection Services { get; }
    internal readonly BusPipelineBuilder Pipeline = new();
    internal readonly List<Assembly> ConsumerAssemblies = new();

    public BusOptions(IServiceCollection services) => Services = services;

    public BusOptions AddConsumersFromAssembly(Assembly a) { ConsumerAssemblies.Add(a); return this; }
    public BusOptions Configure(Action<BusPipelineBuilder> p) { p(Pipeline); return this; }

    internal void ApplyPipelineDefaults()
    {
        // Порядок дефолтного пайплайна
        Pipeline.Use<TelemetryMiddleware>();
        Pipeline.Use<ScopeMiddleware>();
        Pipeline.Use<TenantMiddleware>();
        Pipeline.Use<InboxDedupMiddleware>();
        Pipeline.Use<RecoverabilityMiddleware>();
        Pipeline.Use<HandlerInvokerMiddleware>();   // терминальный
    }
}
```

Ядро самодостаточно и не тянет транспорты — их подключают опциональные пакеты (см. файл 18).
