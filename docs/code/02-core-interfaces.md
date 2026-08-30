# AvtoBus.Core — Интерфейсы шины, транспорта и пайплайна

> **Code sketch / unverified.** API может конфликтовать с другими эскизами. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Core/IBus.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Главная точка входа в шину.
/// Публикация событий, отправка команд, request/response, планирование.
/// </summary>
public interface IBus
{
    /// <summary>
    /// Опубликовать событие (0..N подписчиков).
    /// </summary>
    ValueTask Publish<T>(
        T @event,
        PublishOptions? options = null,
        CancellationToken ct = default) where T : class;

    /// <summary>
    /// Отправить команду (ровно 1 получатель).
    /// </summary>
    ValueTask Send<T>(
        T command,
        SendOptions? options = null,
        CancellationToken ct = default) where T : class;

    /// <summary>
    /// Request/Response: отправить запрос и ждать ответ.
    /// </summary>
    ValueTask<TReply> Request<T, TReply>(
        T request,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
        where T : class
        where TReply : class;

    /// <summary>
    /// Запланировать отложенное сообщение.
    /// </summary>
    ValueTask<Guid> Schedule<T>(
        T message,
        DateTimeOffset at,
        CancellationToken ct = default) where T : class;

    /// <summary>
    /// Запланировать отложенное сообщение по задержке.
    /// </summary>
    ValueTask<Guid> Schedule<T>(
        T message,
        TimeSpan delay,
        CancellationToken ct = default) where T : class;

    /// <summary>
    /// Отменить запланированное сообщение.
    /// </summary>
    ValueTask CancelScheduled(
        Guid token,
        CancellationToken ct = default);

    /// <summary>
    /// Отправить сообщение локально (внутри процесса, без брокера).
    /// </summary>
    ValueTask EnqueueLocal<T>(
        T message,
        CancellationToken ct = default) where T : class;
}
```

---

## AvtoBus.Core/IBusContextAccessor.cs

```csharp
using System.Diagnostics;

namespace AvtoBus;

/// <summary>
/// Доступ к текущему контексту обработки.
/// Работает аналогично IHttpContextAccessor.
/// </summary>
public interface IBusContextAccessor
{
    ConsumeContext? Current { get; set; }
}

/// <summary>
/// Реализация через AsyncLocal — изолирована по потокам и async-цепочкам.
/// </summary>
internal sealed class AsyncLocalBusContextAccessor : IBusContextAccessor
{
    private static readonly AsyncLocal<ConsumeContext?> _current = new();

    public ConsumeContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}
```

---

## AvtoBus.Core/ConsumeContext.cs

```csharp
using System.Runtime.CompilerServices;

namespace AvtoBus;

/// <summary>
/// Контекст обработки сообщения — аналог HttpContext для шины.
/// Содержит всё, что нужно хендлеру и middleware.
/// </summary>
public class ConsumeContext
{
    private readonly List<OutgoingItem> _outgoing = new();
    private readonly Dictionary<string, object?> _baggage = new();

    /// <summary>
    /// Конверт сообщения.
    /// </summary>
    public required Envelope Envelope { get; init; }

    /// <summary>
    /// Десериализованное тело сообщения.
    /// </summary>
    public required object Message { get; init; }

    /// <summary>
    /// DI-провайдер, изолированный per-consume scope.
    /// </summary>
    public required IServiceProvider Services { get; init; }

    /// <summary>
    /// CancellationToken обработки.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Текущая попытка обработки (0-based).
    /// </summary>
    public int Attempt => Envelope.DeliveryAttempt;

    /// <summary>
    /// Хранилище данных между middleware (аналог HttpContext.Items).
    /// </summary>
    public IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();

    /// <summary>
    /// Baggage — сквозные данные через всю цепочку сообщений (W3C Baggage).
    /// </summary>
    public IReadOnlyDictionary<string, string> Baggage => _baggage;

    /// <summary>
    /// Время начала обработки (для метрик).
    /// </summary>
    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;

    // ── Исходящие сообщения ──
    internal IReadOnlyList<OutgoingItem> Outgoing => _outgoing;

    /// <summary>
    /// Кэш десериализованных типизированных версий Message.
    /// </summary>
    private readonly Dictionary<Type, object?> _typedCache = new();

    /// <summary>
    /// Получить типизированное тело сообщения.
    /// </summary>
    public T MessageAs<T>() where T : class
    {
        if (_typedCache.TryGetValue(typeof(T), out var cached))
            return (T)cached!;

        if (Message is T typed)
        {
            _typedCache[typeof(T)] = typed;
            return typed;
        }

        throw new InvalidCastException(
            $"Message type is {Message.GetType().Name}, cannot cast to {typeof(T).Name}");
    }

    /// <summary>
    /// Опубликовать событие из хендлера (каскад).
    /// </summary>
    public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null) where T : class
    {
        _outgoing.Add(new OutgoingItem(@event, OutgoingKind.Publish, options));
        return default;
    }

    /// <summary>
    /// Отправить команду из хендлера (каскад).
    /// </summary>
    public ValueTask SendAsync<T>(T command, SendOptions? options = null) where T : class
    {
        _outgoing.Add(new OutgoingItem(command, OutgoingKind.Send, options));
        return default;
    }

    /// <summary>
    /// Ответить на request (только для Request/Response сценариев).
    /// </summary>
    public ValueTask RespondAsync<T>(T reply) where T : class
    {
        if (Envelope.ReplyTo is null)
            throw new InvalidOperationException(
                "Cannot respond: ReplyTo is not set. " +
                "Use IBus.Request<T,TReply>() instead.");

        _outgoing.Add(new OutgoingItem(reply, OutgoingKind.Reply, null));
        return default;
    }

    /// <summary>
    /// Отложить повторную обработку (через DLQ с задержкой).
    /// </summary>
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        var sink = Services.GetService<IDeferralSink>();
        if (sink is null)
            throw new InvalidOperationException(
                "IDeferralSink is not registered. Enable outbox or scheduling.");

        return sink.DeferAsync(Envelope, delay, ct);
    }

    /// <summary>
    /// Пометить сообщение как мёртвое (отправить в DLQ).
    /// </summary>
    public void DeadLetter(string reason)
    {
        throw new DeadLetterException(Envelope, reason);
    }

    /// <summary>
    /// Продолжить жить — вызывать в долгих хендлерах для продления lock.
    /// </summary>
    public ValueTask KeepAliveAsync(CancellationToken ct = default)
    {
        var sink = Services.GetService<IKeepAliveSink>();
        return sink?.KeepAliveAsync(Envelope, ct) ?? default;
    }

    /// <summary>
    /// Установить Baggage.
    /// </summary>
    public void SetBaggage(string key, string value) => _baggage[key] = value;

    /// <summary>
    /// Получить baggage или значение по умолчанию.
    /// </summary>
    public string? GetBaggage(string key) =>
        _baggage.TryGetValue(key, out var val) ? val : null;

    internal void AddOutgoing(OutgoingItem item) => _outgoing.Add(item);
}

/// <summary>
/// Типизированный контекст обработки.
/// </summary>
public sealed class ConsumeContext<T> : ConsumeContext where T : class
{
    public new required T Message { get; init; }
}
```

---

## AvtoBus.Core/Pipeline/IBusMiddleware.cs

```csharp
namespace AvtoBus.Pipeline;

/// <summary>
/// Delegate-терминатор пайплайна.
/// </summary>
public delegate ValueTask BusDelegate(ConsumeContext context);

/// <summary>
/// Middleware — шаг пайплайна обработки.
/// Используйте «русскую матрёшку»: вызовите next(context) чтобы передать управление дальше.
/// </summary>
public interface IBusMiddleware
{
    ValueTask InvokeAsync(ConsumeContext context, BusDelegate next);
}

/// <summary>
/// Inline-\Middleware: принимает делегат (lambda).
/// </summary>
internal sealed class InlineMiddleware : IBusMiddleware
{
    private readonly Func<ConsumeContext, BusDelegate, ValueTask> _invoke;

    public InlineMiddleware(Func<ConsumeContext, BusDelegate, ValueTask> invoke)
        => _invoke = invoke;

    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
        => _invoke(context, next);
}
```

---

## AvtoBus.Core/Pipeline/BusPipelineBuilder.cs

```csharp
namespace AvtoBus.Pipeline;

/// <summary>
/// Строитель цепочки middleware.
/// Каждый Use() оборачивает предыдущего в «матрёшке».
/// </summary>
public sealed class BusPipelineBuilder
{
    private readonly List<Func<BusDelegate, BusDelegate>> _components = new();

    /// <summary>
    /// Добавить middleware по типу из DI.
    /// </summary>
    public BusPipelineBuilder Use<TMiddleware>() where TMiddleware : class, IBusMiddleware
    {
        _components.Add(next => async ctx =>
        {
            var middleware = ctx.Services.GetRequiredService<TMiddleware>();
            await middleware.InvokeAsync(ctx, next);
        });
        return this;
    }

    /// <summary>
    /// Добавить inline middleware.
    /// </summary>
    public BusPipelineBuilder Use(Func<ConsumeContext, BusDelegate, ValueTask> invoke)
    {
        _components.Add(next => ctx => invoke(ctx, next));
        return this;
    }

    /// <summary>
    /// Добавить middleware условно.
    /// </summary>
    public BusPipelineBuilder UseWhen(
        Predicate<ConsumeContext> condition,
        Action<BusPipelineBuilder> branch)
    {
        var innerBuilder = new BusPipelineBuilder();
        branch(innerBuilder);
        var innerDelegate = innerBuilder.Build(_ => default);

        _components.Add(next => async ctx =>
        {
            if (condition(ctx))
                await innerDelegate(ctx);
            await next(ctx);
        });
        return this;
    }

    /// <summary>
    /// Собрать пайплайн из цепочки middleware + терминальный обработчик.
    /// </summary>
    public BusDelegate Build(BusDelegate terminal)
    {
        var pipeline = terminal;

        for (var i = _components.Count - 1; i >= 0; i--)
            pipeline = _components[i](pipeline);

        return pipeline;
    }

    /// <summary>
    /// Возвращает количество зарегистрированных middleware.
    /// </summary>
    public int Count => _components.Count;

    /// <summary>
    /// Очистить все middleware (для переопределения дефолтного пайплайна).
    /// </summary>
    public void Clear() => _components.Clear();
}
```

---

## AvtoBus.Core/Pipeline/HandlerInvokerMiddleware.cs

```csharp
namespace AvtoBus.Pipeline;

/// <summary>
/// Терминальный middleware — вызывает зарегистрированный диспетчер.
/// При экстремальной нагрузке или при отсутствии генератора,
/// используется рефлексионный fallback.
/// </summary>
internal sealed class HandlerInvokerMiddleware : IBusMiddleware
{
    private readonly DispatcherRegistry _dispatchers;
    private readonly ILogger<HandlerInvokerMiddleware> _log;

    public HandlerInvokerMiddleware(
        DispatcherRegistry dispatchers,
        ILogger<HandlerInvokerMiddleware> log)
    {
        _dispatchers = dispatchers;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate _)
    {
        // 1. Найти диспетчер
        if (!_dispatchers.TryGet(ctx.Envelope.MessageType, out var dispatcher))
            throw new NoHandlerException(ctx.Envelope.MessageType);

        // 2. Вызвать хендлер
        _log.LogDebug("Dispatching {MessageType} (attempt {Attempt})",
            ctx.Envelope.MessageType, ctx.Attempt);

        await dispatcher.DispatchAsync(ctx);

        // 3. Применить каскадные исходящие
        if (ctx.Outgoing.Count > 0)
        {
            var bus = ctx.Services.GetRequiredService<IBus>();
            foreach (var item in ctx.Outgoing)
            {
                switch (item.Kind)
                {
                    case OutgoingKind.Publish:
                        await bus.Publish(item.Message, item.PublishOptions, ctx.CancellationToken);
                        break;
                    case OutgoingKind.Send:
                        await bus.Send(item.Message, item.SendOptions, ctx.CancellationToken);
                        break;
                    case OutgoingKind.Reply:
                        await ctx.RespondAsync(item.Message);
                        break;
                }
            }
        }
    }
}
```

---

## AvtoBus.Core/Dispatching/IMessageDispatcher.cs

```csharp
namespace AvtoBus.Dispatching;

/// <summary>
/// Диспетчер одного типа сообщения.
/// Генерируется Source Generator'ом, либо создаётся рефлексионно.
/// </summary>
public interface IMessageDispatcher
{
    /// <summary>
    /// Строковый тип сообщения для маршрутизации.
    /// </summary>
    string MessageType { get; }

    /// <summary>
    /// CLR-тип сообщения.
    /// </summary>
    Type ClrType { get; }

    /// <summary>
    /// Вызвать обработчик.
    /// </summary>
    ValueTask DispatchAsync(ConsumeContext context);
}

/// <summary>
/// Генерированный Source Generator'ом dispatch delegate.
/// </summary>
public delegate ValueTask HandlerFunc(ConsumeContext context, object message);
```

---

## AvtoBus.Core/Dispatching/DispatcherRegistry.cs

```csharp
using System.Collections.Frozen;

namespace AvtoBus.Dispatching;

/// <summary>
/// Реестр всех диспетчеров сообщений.
/// Заполняется при старте (Source Generator или рефлексия).
/// </summary>
public sealed class DispatcherRegistry
{
    private readonly FrozenDictionary<string, IMessageDispatcher> _byType;
    private readonly FrozenDictionary<Type, IMessageDispatcher> _byClr;

    public DispatcherRegistry(IEnumerable<IMessageDispatcher> dispatchers)
    {
        var list = dispatchers.ToArray();
        _byType = list.ToFrozenDictionary(d => d.MessageType, StringComparer.OrdinalIgnoreCase);
        _byClr = list.ToFrozenDictionary(d => d.ClrType);
    }

    public bool TryGet(string messageType, out IMessageDispatcher dispatcher)
        => _byType.TryGetValue(messageType, out dispatcher!);

    public bool TryGet(Type clrType, out IMessageDispatcher dispatcher)
        => _byClr.TryGetValue(clrType, out dispatcher!);

    public int Count => _byType.Count;
}
```
