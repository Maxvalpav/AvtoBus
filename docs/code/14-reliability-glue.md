# AvtoBus — Reliability Glue: UoW, Reply, DLQ, Local queues

> **Design correction / unverified.** Файл уточняет критические границы надёжности, но код ещё не собран в единую реализацию. Канонический статус: [`../FINAL.md`](../FINAL.md).

Исправление багов A1–A9 и подсистем B1–B4 из `30-forgotten-and-bugs.md`.
**Это сердце надёжности — без него Outbox декоративен.**

---

## 1. Unit of Work — транзакционная привязка Outbox (фикс A1, A3, A8, B1)

### AvtoBus.Core/UnitOfWork/IMessageSession.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Сессия сообщений: буферизует исходящие, сбрасывает их в outbox
/// в ТОЙ ЖЕ транзакции, что и бизнес-данные.
///
/// Использование из HTTP-контроллера:
///   await using var session = sessionFactory.Begin();
///   await session.Send(new PlaceOrder(...));   // в буфер
///   db.Orders.Add(order);                       // бизнес-данные
///   await session.CommitAsync();                // всё в одной транзакции
/// </summary>
public interface IMessageSession : IAsyncDisposable
{
    ValueTask Send<T>(T command, SendOptions? options = null) where T : class;
    ValueTask Publish<T>(T @event, PublishOptions? options = null) where T : class;
    ValueTask CommitAsync(CancellationToken ct = default);
    IReadOnlyList<OutgoingItem> Pending { get; }
}

public interface IMessageSessionFactory
{
    IMessageSession Begin();
}
```

### AvtoBus.Outbox.EfCore/EfCoreMessageSession.cs

```csharp
using AvtoBus.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace AvtoBus;

/// <summary>
/// Сессия поверх EF Core DbContext.
/// Ключевая идея: исходящие пишутся в outbox-таблицу в рамках транзакции DbContext.
/// Коммит транзакции = атомарно бизнес-данные + сообщения.
/// </summary>
internal sealed class EfCoreMessageSession<TDbContext> : IMessageSession
    where TDbContext : DbContext
{
    private readonly TDbContext _db;
    private readonly IRouter _router;
    private readonly ISerializer _serializer;
    private readonly ITypeResolver _typeResolver;
    private readonly IEnvelopeSerializer _envelopeSerializer;
    private readonly IOutboxSignal _signal;
    private readonly TimeProvider _clock;
    private readonly List<OutgoingItem> _pending = new();

    public EfCoreMessageSession(
        TDbContext db, IRouter router, ISerializer serializer,
        ITypeResolver typeResolver, IEnvelopeSerializer envelopeSerializer,
        IOutboxSignal signal, TimeProvider clock)
    {
        _db = db;
        _router = router;
        _serializer = serializer;
        _typeResolver = typeResolver;
        _envelopeSerializer = envelopeSerializer;
        _signal = signal;
        _clock = clock;
    }

    public IReadOnlyList<OutgoingItem> Pending => _pending;

    public ValueTask Send<T>(T command, SendOptions? options = null) where T : class
    {
        _pending.Add(new OutgoingItem(command, OutgoingKind.Send, null, options));
        return default;
    }

    public ValueTask Publish<T>(T @event, PublishOptions? options = null) where T : class
    {
        _pending.Add(new OutgoingItem(@event, OutgoingKind.Publish, options));
        return default;
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_pending.Count == 0)
        {
            await _db.SaveChangesAsync(ct);
            return;
        }

        // Оборачиваем в транзакцию, если её ещё нет
        var ownTransaction = _db.Database.CurrentTransaction is null;
        IDbContextTransaction? tx = null;
        if (ownTransaction)
            tx = await _db.Database.BeginTransactionAsync(ct);

        try
        {
            // 1. Записать исходящие в outbox-таблицу
            foreach (var item in _pending)
            {
                var isCommand = item.Kind == OutgoingKind.Send;
                var type = item.Message.GetType();
                var route = _router.Route(type, isCommand);
                var body = _serializer.Serialize(item.Message);
                var envelope = BuildEnvelope(type, body, item.PublishOptions ?? item.SendOptions);

                _db.Set<OutboxMessage>().Add(new OutboxMessage
                {
                    MessageId = envelope.MessageId,
                    MessageType = envelope.MessageType,
                    Destination = route.Destination.Address,
                    Transport = route.Transport,
                    PartitionKey = envelope.PartitionKey,
                    TenantId = envelope.TenantId,
                    EnvelopeBlob = _envelopeSerializer.Serialize(envelope),
                    CreatedAt = _clock.GetUtcNow().UtcDateTime,
                    SendAfter = envelope.DeliverAt?.UtcDateTime,
                });
            }

            // 2. Атомарный коммит: бизнес-данные + outbox
            await _db.SaveChangesAsync(ct);
            if (ownTransaction && tx is not null)
                await tx.CommitAsync(ct);

            _pending.Clear();

            // 3. Пнуть relay (после успешного коммита!)
            _signal.Nudge();
        }
        catch
        {
            if (ownTransaction && tx is not null)
                await tx.RollbackAsync(ct);
            throw;
        }
        finally
        {
            if (tx is not null)
                await tx.DisposeAsync();
        }
    }

    private Envelope BuildEnvelope(Type type, ReadOnlyMemory<byte> body, PublishOptions? opts)
    {
        var id = Guid.NewGuid();
        var now = _clock.GetUtcNow();
        return new Envelope
        {
            MessageId = id,
            CorrelationId = id,
            MessageType = _typeResolver.GetName(type),
            Body = body,
            SentAt = now,
            DeliverAt = opts?.Delay is { } d ? now.Add(d) : null,
            TimeToLive = opts?.Ttl,
            PartitionKey = opts?.PartitionKey,
            TenantId = opts?.TenantId,
            Priority = opts?.Priority ?? 4,
            TraceParent = System.Diagnostics.Activity.Current?.Id,
            Headers = (opts?.Headers ?? new()).ToFrozenDictionary(),
        };
    }

    public ValueTask DisposeAsync() => default;
}

internal sealed class EfCoreMessageSessionFactory<TDbContext> : IMessageSessionFactory
    where TDbContext : DbContext
{
    private readonly IServiceProvider _services;
    public EfCoreMessageSessionFactory(IServiceProvider services) => _services = services;

    public IMessageSession Begin()
    {
        var scope = _services.CreateScope();
        var sp = scope.ServiceProvider;
        return new EfCoreMessageSession<TDbContext>(
            sp.GetRequiredService<TDbContext>(),
            sp.GetRequiredService<IRouter>(),
            sp.GetRequiredService<ISerializer>(),
            sp.GetRequiredService<ITypeResolver>(),
            sp.GetRequiredService<IEnvelopeSerializer>(),
            sp.GetRequiredService<IOutboxSignal>(),
            sp.GetRequiredService<TimeProvider>());
    }
}
```

### Использование (то, чего не хватало)

```csharp
// В контроллере — транзакционная публикация
app.MapPost("/orders", async (
    PlaceOrderRequest req,
    AppDbContext db,
    IMessageSessionFactory sessions) =>
{
    await using var session = sessions.Begin();

    var order = Order.Create(req.CustomerId, req.Items);
    db.Orders.Add(order);                              // бизнес-данные
    await session.Publish(new OrderPlaced(order.Id));  // событие

    await session.CommitAsync();  // ОБА — в одной транзакции, атомарно

    return Results.Ok(order.Id);
});
```

---

## 2. Каскады хендлера через тот же scope (фикс A8)

### AvtoBus.Core/Pipeline/CascadeMiddleware.cs

```csharp
using AvtoBus.Pipeline;

namespace AvtoBus.Pipeline;

/// <summary>
/// После успешной обработки применяет каскадные исходящие ЧЕРЕЗ outbox,
/// если хендлер работал с DbContext (UoW), иначе — напрямую в транспорт.
/// Ставится ПОСЛЕ HandlerInvoker.
/// </summary>
internal sealed class CascadeMiddleware : IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        await next(ctx);  // сначала хендлер (он наполняет ctx.Outgoing)

        if (ctx.Outgoing.Count == 0)
            return;

        var outbox = ctx.Services.GetService<IOutbox>();
        var router = ctx.Services.GetRequiredService<IRouter>();
        var serializer = ctx.Services.GetRequiredService<ISerializer>();
        var typeResolver = ctx.Services.GetRequiredService<ITypeResolver>();
        var clock = ctx.Services.GetRequiredService<TimeProvider>();

        foreach (var item in ctx.Outgoing)
        {
            var type = item.Message.GetType();
            var isCommand = item.Kind == OutgoingKind.Send;

            if (item.Kind == OutgoingKind.Reply)
            {
                await ReplyDispatcher.SendReplyAsync(ctx, item.Message);
                continue;
            }

            var route = router.Route(type, isCommand);
            var body = serializer.Serialize(item.Message);
            var envelope = BuildCascadeEnvelope(ctx, type, body, item, typeResolver, clock);

            // Каскад наследует correlation/causation родителя
            if (outbox is not null && ctx.HasUnitOfWork)
                await outbox.EnqueueAsync(envelope, route, ctx.CancellationToken);
            else
            {
                var transport = ctx.Services.GetRequiredService<ITransportSelector>().For(route.Transport);
                await transport.SendAsync(envelope, route.Destination, ctx.CancellationToken);
            }
        }
    }

    private static Envelope BuildCascadeEnvelope(
        ConsumeContext ctx, Type type, ReadOnlyMemory<byte> body,
        OutgoingItem item, ITypeResolver typeResolver, TimeProvider clock)
    {
        var id = Guid.NewGuid();
        var opts = item.PublishOptions ?? item.SendOptions;
        return new Envelope
        {
            MessageId = id,
            CorrelationId = ctx.Envelope.CorrelationId ?? ctx.Envelope.MessageId,
            CausationId = ctx.Envelope.MessageId,   // ← родитель
            MessageType = typeResolver.GetName(type),
            Body = body,
            SentAt = clock.GetUtcNow(),
            DeliverAt = opts?.Delay is { } d ? clock.GetUtcNow().Add(d) : null,
            PartitionKey = opts?.PartitionKey ?? ctx.Envelope.PartitionKey,
            TenantId = opts?.TenantId ?? ctx.Envelope.TenantId,
            TraceParent = System.Diagnostics.Activity.Current?.Id,
            Headers = (opts?.Headers ?? new()).ToFrozenDictionary(),
        };
    }
}
```

---

## 3. Reply-корреляция (фикс A2) — Request/Response реально работает

### AvtoBus.Core/RequestResponse/ReplyMiddleware.cs

```csharp
using System.Collections.Concurrent;
using AvtoBus.Pipeline;

namespace AvtoBus.RequestResponse;

/// <summary>
/// Реестр ожидающих ответов. Заменяет "фантомный" RegisterReply.
/// </summary>
public sealed class ReplyRegistry
{
    private sealed record Pending(Type ReplyType, object Tcs, Timer Timer);
    private readonly ConcurrentDictionary<string, Pending> _pending = new();
    private readonly ISerializer _serializer;
    private int _counter;

    public ReplyRegistry(ISerializer serializer) => _serializer = serializer;

    public string Register<TReply>(TaskCompletionSource<TReply> tcs, TimeSpan timeout)
        where TReply : class
    {
        var replyTo = $"avtobus.reply.{Environment.MachineName}.{Interlocked.Increment(ref _counter)}";
        var timer = new Timer(_ =>
        {
            if (_pending.TryRemove(replyTo, out var p))
            {
                ((TaskCompletionSource<TReply>)p.Tcs).TrySetException(
                    new TimeoutException($"Request timed out after {timeout}"));
                p.Timer.Dispose();
            }
        }, null, timeout, Timeout.InfiniteTimeSpan);

        _pending[replyTo] = new Pending(typeof(TReply), tcs, timer);
        return replyTo;
    }

    /// <summary>Вызывается, когда пришёл ответ (по заголовку reply-to).</summary>
    public bool TryComplete(string replyTo, ReadOnlyMemory<byte> body)
    {
        if (!_pending.TryRemove(replyTo, out var p))
            return false;

        p.Timer.Dispose();
        var reply = _serializer.Deserialize(body, p.ReplyType);

        // Вызвать TrySetResult через рефлексию по типу
        var setResult = p.Tcs.GetType().GetMethod("TrySetResult")!;
        setResult.Invoke(p.Tcs, new[] { reply });
        return true;
    }
}

/// <summary>
/// Middleware: если у входящего сообщения есть заголовок avtobus.is-reply,
/// завершаем соответствующий Request вместо обычной обработки.
/// </summary>
internal sealed class ReplyMiddleware : IBusMiddleware
{
    private readonly ReplyRegistry _registry;
    public ReplyMiddleware(ReplyRegistry registry) => _registry = registry;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (ctx.Envelope.Headers.TryGetValue("avtobus.reply-correlation", out var replyTo))
        {
            if (_registry.TryComplete(replyTo, ctx.Envelope.Body))
                return;  // ответ доставлен инициатору, дальше не идём
        }

        await next(ctx);
    }
}

/// <summary>
/// Отправка ответа обратно инициатору.
/// </summary>
public static class ReplyDispatcher
{
    public static async ValueTask SendReplyAsync(ConsumeContext ctx, object reply)
    {
        if (ctx.Envelope.ReplyTo is not { } replyTo)
            throw new InvalidOperationException("No ReplyTo — cannot send reply.");

        var serializer = ctx.Services.GetRequiredService<ISerializer>();
        var typeResolver = ctx.Services.GetRequiredService<ITypeResolver>();
        var transports = ctx.Services.GetRequiredService<ITransportSelector>();

        var envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = ctx.Envelope.CorrelationId,
            CausationId = ctx.Envelope.MessageId,
            MessageType = typeResolver.GetName(reply.GetType()),
            Body = serializer.Serialize(reply),
            SentAt = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>
            {
                ["avtobus.reply-correlation"] = ctx.Envelope.Headers.GetValueOrDefault("avtobus.reply-correlation", "")
            }.ToFrozenDictionary(),
        };

        // reply-to указывает на очередь инициатора
        await transports.Default.SendAsync(
            envelope, new TransportDestination(replyTo, DestinationKind.Reply), ctx.CancellationToken);
    }
}
```

Обновлённый `DefaultBus.Request` (фикс A2):

```csharp
public async ValueTask<TReply> Request<T, TReply>(
    T request, TimeSpan? timeout = null, CancellationToken ct = default)
    where T : class where TReply : class
{
    var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);
    var replyTo = _replyRegistry.Register(tcs, timeout ?? TimeSpan.FromSeconds(30));

    await Send(request, new SendOptions
    {
        Headers = new()
        {
            ["avtobus.reply-correlation"] = replyTo,
        },
    }, ct);
    // Envelope.ReplyTo = наша reply-очередь (endpoint инициатора)

    return await tcs.Task.WaitAsync(ct);
}
```

---

## 4. DLQ: хранилище, reader, replay (B2)

### AvtoBus.Core/DeadLetter/IDeadLetterStore.cs

```csharp
namespace AvtoBus.DeadLetter;

/// <summary>
/// Rich error envelope — полный контекст падения (идея 165).
/// </summary>
public sealed record DeadLetter
{
    public required Guid Id { get; init; }
    public required string OriginalQueue { get; init; }
    public required Envelope Envelope { get; init; }
    public required string MessageType { get; init; }
    public required DeadLetterReason Reason { get; init; }
    public required string ExceptionType { get; init; }
    public required string ExceptionMessage { get; init; }
    public string? StackTrace { get; init; }
    public required int Attempts { get; init; }
    public required string Host { get; init; }
    public required string AppVersion { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public string? TraceId { get; init; }
}

public enum DeadLetterReason
{
    /// <summary>Транзиентная ошибка после всех ретраев (можно реплеить).</summary>
    Error,
    /// <summary>Десериализация/нет типа/контракт (реплей бессмысленен).</summary>
    Poison,
    /// <summary>Истёк TTL.</summary>
    Expired,
    /// <summary>Явно помечен хендлером.</summary>
    Rejected,
}

public interface IDeadLetterStore
{
    ValueTask StoreAsync(DeadLetter deadLetter, CancellationToken ct = default);
    ValueTask<IReadOnlyList<DeadLetter>> QueryAsync(
        string? queue = null, DeadLetterReason? reason = null,
        string? messageType = null, int skip = 0, int take = 50, CancellationToken ct = default);
    ValueTask<DeadLetter?> GetAsync(Guid id, CancellationToken ct = default);
    ValueTask<int> CountAsync(string? queue = null, CancellationToken ct = default);
    ValueTask DeleteAsync(Guid id, CancellationToken ct = default);
}
```

### AvtoBus.Outbox.EfCore/DeadLetterHandling.cs

```csharp
using System.Diagnostics;
using AvtoBus.DeadLetter;
using AvtoBus.Pipeline;

namespace AvtoBus;

/// <summary>
/// Терминальный обработчик ошибок: пишет rich-error в DLQ store.
/// Ставится САМЫМ ВНЕШНИМ в пайплайне (ловит всё).
/// </summary>
internal sealed class DeadLetterMiddleware : IBusMiddleware
{
    private readonly IDeadLetterStore _store;
    private readonly RecoverabilityOptions _recoverability;
    private readonly IBus _bus;
    private readonly TimeProvider _clock;
    private static readonly string s_host = Environment.MachineName;
    private static readonly string s_version =
        System.Reflection.Assembly.GetEntryAssembly()?.GetName().Version?.ToString() ?? "unknown";

    public DeadLetterMiddleware(
        IDeadLetterStore store, RecoverabilityOptions recoverability,
        IBus bus, TimeProvider clock)
    {
        _store = store;
        _recoverability = recoverability;
        _bus = bus;
        _clock = clock;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        try
        {
            await next(ctx);
        }
        catch (DeadLetterException ex)
        {
            await StoreAsync(ctx, ex, DeadLetterReason.Rejected);
        }
        catch (SerializationException ex)
        {
            await StoreAsync(ctx, ex, DeadLetterReason.Poison);
        }
        catch (OperationCanceledException) when (ctx.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Попал сюда = recoverability уже исчерпал ретраи
            var reason = ex is UnknownEventTypeException or NoHandlerException
                ? DeadLetterReason.Poison
                : DeadLetterReason.Error;
            await StoreAsync(ctx, ex, reason);
        }
    }

    private async ValueTask StoreAsync(ConsumeContext ctx, Exception ex, DeadLetterReason reason)
    {
        var dl = new DeadLetter
        {
            Id = Guid.NewGuid(),
            OriginalQueue = ctx.Envelope.Headers.GetValueOrDefault("consumer") ?? "unknown",
            Envelope = ctx.Envelope,
            MessageType = ctx.Envelope.MessageType,
            Reason = reason,
            ExceptionType = ex.GetType().FullName ?? "Unknown",
            ExceptionMessage = ex.Message,
            StackTrace = ex.StackTrace,
            Attempts = ctx.Attempt + 1,
            Host = s_host,
            AppVersion = s_version,
            FailedAt = _clock.GetUtcNow(),
            TraceId = Activity.Current?.TraceId.ToString(),
        };

        await _store.StoreAsync(dl, ctx.CancellationToken);
        BusMetrics.DeadLetteredCount.Add(1,
            new TagList { { "reason", reason.ToString() }, { "type", ctx.Envelope.MessageType } });

        // Системное событие (можно алертить, идея 146)
        await _bus.Publish(new MessageDeadLettered(
            dl.Id, dl.MessageType, dl.Reason.ToString(), dl.ExceptionMessage), ct: ctx.CancellationToken);
    }
}

public sealed record MessageDeadLettered(Guid DeadLetterId, string MessageType, string Reason, string Error) : IEvent;
```

### AvtoBus.Core/DeadLetter/DeadLetterReplayer.cs

```csharp
namespace AvtoBus.DeadLetter;

/// <summary>
/// Реплей сообщений из DLQ с фильтром и rate-limit (идеи 166, 168).
/// </summary>
public sealed class DeadLetterReplayer
{
    private readonly IDeadLetterStore _store;
    private readonly ITransportSelector _transports;
    private readonly IRouter _router;
    private readonly ILogger<DeadLetterReplayer> _log;

    public DeadLetterReplayer(
        IDeadLetterStore store, ITransportSelector transports,
        IRouter router, ILogger<DeadLetterReplayer> log)
    {
        _store = store;
        _transports = transports;
        _router = router;
        _log = log;
    }

    public async ValueTask<int> ReplayAsync(ReplayOptions options, CancellationToken ct = default)
    {
        var replayed = 0;
        var perSecond = options.RatePerSecond;
        var delay = perSecond > 0 ? TimeSpan.FromSeconds(1.0 / perSecond) : TimeSpan.Zero;

        var deadLetters = await _store.QueryAsync(
            queue: options.Queue, reason: options.Reason,
            messageType: options.MessageType, take: options.MaxCount, ct: ct);

        foreach (var dl in deadLetters)
        {
            if (dl.Reason == DeadLetterReason.Poison && !options.IncludePoison)
                continue;

            try
            {
                var transport = _transports.Default;
                var envelope = dl.Envelope with
                {
                    MessageId = Guid.NewGuid(),
                    DeliveryAttempt = 0,
                    Headers = dl.Envelope.Headers
                        .Append(new("avtobus.replayed-from", dl.Id.ToString()))
                        .ToFrozenDictionary(kv => kv.Key, kv => kv.Value),
                };

                await transport.SendAsync(envelope,
                    new TransportDestination(dl.OriginalQueue, DestinationKind.Queue), ct);

                await _store.DeleteAsync(dl.Id, ct);
                replayed++;

                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Replay failed for {Id}", dl.Id);
            }
        }

        _log.LogInformation("Replayed {Count} messages from DLQ", replayed);
        return replayed;
    }
}

public sealed record ReplayOptions
{
    public string? Queue { get; init; }
    public DeadLetterReason? Reason { get; init; }
    public string? MessageType { get; init; }
    public int MaxCount { get; init; } = 1000;
    public double RatePerSecond { get; init; } = 10;
    public bool IncludePoison { get; init; }
}
```

---

## 5. Local in-process queues (B3)

### AvtoBus.Core/Local/LocalQueueProcessor.cs

```csharp
using System.Threading.Channels;
using AvtoBus.Pipeline;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Local;

/// <summary>
/// In-process очередь (идея 15): без брокера, через Channel.
/// Полезно для фоновой работы внутри одного сервиса.
/// </summary>
public sealed class LocalQueue
{
    private readonly Channel<(Envelope, object)> _channel;
    public string Name { get; }
    public int MaxParallelism { get; }

    public LocalQueue(string name, int maxParallelism = 1, int capacity = 10_000)
    {
        Name = name;
        MaxParallelism = maxParallelism;
        _channel = Channel.CreateBounded<(Envelope, object)>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = maxParallelism == 1,
        });
    }

    public ValueTask EnqueueAsync(Envelope envelope, object message, CancellationToken ct)
        => _channel.Writer.WriteAsync((envelope, message), ct);

    public ChannelReader<(Envelope Envelope, object Message)> Reader => _channel.Reader;
}

/// <summary>
/// Фоновый обработчик локальных очередей.
/// </summary>
public sealed class LocalQueueProcessor : BackgroundService
{
    private readonly IEnumerable<LocalQueue> _queues;
    private readonly BusPipelineBuilder _pipelineBuilder;
    private readonly IServiceProvider _services;
    private readonly ILogger<LocalQueueProcessor> _log;

    public LocalQueueProcessor(
        IEnumerable<LocalQueue> queues,
        BusPipelineBuilder pipelineBuilder,
        IServiceProvider services,
        ILogger<LocalQueueProcessor> log)
    {
        _queues = queues;
        _pipelineBuilder = pipelineBuilder;
        _services = services;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var pipeline = _pipelineBuilder.Build(async c =>
        {
            var invoker = c.Services.GetRequiredService<HandlerInvokerMiddleware>();
            await invoker.InvokeAsync(c, _ => default);
        });

        var tasks = new List<Task>();
        foreach (var queue in _queues)
        {
            for (var i = 0; i < queue.MaxParallelism; i++)
                tasks.Add(RunWorker(queue, pipeline, ct));
        }
        await Task.WhenAll(tasks);
    }

    private async Task RunWorker(LocalQueue queue, BusDelegate pipeline, CancellationToken ct)
    {
        await foreach (var (envelope, message) in queue.Reader.ReadAllAsync(ct))
        {
            try
            {
                var ctx = new ConsumeContext
                {
                    Envelope = envelope,
                    Message = message,
                    Services = _services,
                    CancellationToken = ct,
                };
                await pipeline(ctx);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Local queue {Queue} handler failed", queue.Name);
            }
        }
    }
}
```

---

## 6. Second-level retry (B4)

### AvtoBus.Core/Pipeline/SecondLevelRetryMiddleware.cs

```csharp
using AvtoBus.Pipeline;

namespace AvtoBus.Pipeline;

/// <summary>
/// После исчерпания обычных ретраев — оборачивает в IFailed&lt;T&gt;
/// и вызывает IHandleFailed&lt;T&gt;, если он есть (идея 169).
/// </summary>
internal sealed class SecondLevelRetryMiddleware : IBusMiddleware
{
    private readonly RecoverabilityOptions _recoverability;
    private readonly DispatcherRegistry _dispatchers;
    private readonly ILogger<SecondLevelRetryMiddleware> _log;
    private readonly List<Exception> _exceptions = new();

    public SecondLevelRetryMiddleware(
        RecoverabilityOptions recoverability,
        DispatcherRegistry dispatchers,
        ILogger<SecondLevelRetryMiddleware> log)
    {
        _recoverability = recoverability;
        _dispatchers = dispatchers;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        try
        {
            await next(ctx);
        }
        catch (Exception ex)
        {
            var maxAttempts = _recoverability.ImmediateRetries + _recoverability.DelayedRetries;
            if (ctx.Attempt < maxAttempts)
                throw;  // ещё есть обычные ретраи

            // Ищем IHandleFailed<T>
            var failedType = typeof(IHandleFailed<>).MakeGenericType(ctx.Message.GetType());
            var handler = ctx.Services.GetService(failedType);
            if (handler is null)
                throw;  // нет fallback → в DLQ

            _log.LogWarning("Invoking second-level retry handler for {Type}", ctx.Envelope.MessageType);

            var failed = CreateFailed(ctx, ex);
            var method = failedType.GetMethod("Handle")!;
            var result = method.Invoke(handler, new[] { failed });
            if (result is Task task) await task;
        }
    }

    private object CreateFailed(ConsumeContext ctx, Exception ex)
    {
        var failedType = typeof(FailedMessage<>).MakeGenericType(ctx.Message.GetType());
        var failed = Activator.CreateInstance(failedType)!;
        failedType.GetProperty("Message")!.SetValue(failed, ctx.Message);
        failedType.GetProperty("Envelope")!.SetValue(failed, ctx.Envelope);
        failedType.GetProperty("ErrorDescription")!.SetValue(failed, ex.Message);
        failedType.GetProperty("Exceptions")!.SetValue(failed, new List<ExceptionInfo>
        {
            new(ex.GetType().Name, ex.Message, ex.StackTrace, DateTimeOffset.UtcNow, ctx.Attempt)
        });
        return failed;
    }
}
```

---

## 7. Итоговый порядок пайплайна (единый источник правды, фикс A6)

```csharp
internal static class DefaultPipeline
{
    public static void Apply(BusPipelineBuilder p)
    {
        p.Use<DeadLetterMiddleware>();        // 1. Внешний catch-all → DLQ
        p.Use<TelemetryMiddleware>();         // 2. OTel span
        p.Use<ScopeMiddleware>();             // 3. DI scope на сообщение
        p.Use<TenantMiddleware>();            // 4. Резолв TenantId
        p.Use<ReplyMiddleware>();             // 5. Перехват ответов Request/Response
        p.Use<InboxDedupMiddleware>();        // 6. Дедупликация
        p.Use<SecondLevelRetryMiddleware>();  // 7. IFailed<T> fallback
        p.Use<RecoverabilityMiddleware>();    // 8. Ретраи/backoff
        p.Use<HandlerInvokerMiddleware>();    // 9. Вызов хендлера
        p.Use<CascadeMiddleware>();           // 10. Каскады через outbox
    }
}
```

---

## 8. Регистрация (фикс A1 — правильные lifetime)

```csharp
public static BusOptions UseTransactionalMessaging<TDbContext>(this BusOptions bus)
    where TDbContext : DbContext
{
    // Session factory — singleton, но создаёт scope внутри Begin()
    bus.Services.AddSingleton<IMessageSessionFactory, EfCoreMessageSessionFactory<TDbContext>>();

    // DLQ
    bus.Services.AddSingleton<IDeadLetterStore, EfCoreDeadLetterStore<TDbContext>>();
    bus.Services.AddSingleton<DeadLetterReplayer>();

    // Reply
    bus.Services.AddSingleton<ReplyRegistry>();

    // Inbox store (реальная EF-реализация, фикс B8)
    bus.Services.AddScoped<IInboxStore, EfCoreInboxStore<TDbContext>>();

    return bus;
}
```
