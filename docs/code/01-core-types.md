# AvtoBus.Core — Типы и абстракции

> **Code sketch / unverified.** Фрагменты не входят в `.csproj` и отдельно не компилировались. Канонический статус: [`../FINAL.md`](../FINAL.md).

Полный исходный код ядра фреймворка.

---

## AvtoBus.Core/Envelope.cs

```csharp
using System.Collections.Frozen;
using System.Diagnostics;
using System.Text.Json;

namespace AvtoBus;

/// <summary>
/// Конверт сообщения — единая обёртка, которая едет по шине.
/// Тело — байты, сериализация/десериализация — на границах пайплайна.
/// </summary>
public sealed record Envelope
{
    public required Guid MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public required string MessageType { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public string ContentType { get; init; } = "application/json";
    public string ContentEncoding { get; init; } = "identity";

    public DateTimeOffset SentAt { get; init; }
    public DateTimeOffset? DeliverAt { get; init; }
    public DateTimeOffset? Deadline { get; init; }
    public TimeSpan? TimeToLive { get; init; }

    public string? PartitionKey { get; init; }
    public string? TenantId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Source { get; init; }
    public string? Consumer { get; init; }

    public byte Priority { get; init; } = 4;
    public int DeliveryAttempt { get; init; }

    public string? TraceParent { get; init; }
    public string? TraceState { get; init; }

    public IReadOnlyDictionary<string, string> Headers { get; init; }
        = FrozenDictionary<string, string>.Empty;

    // ── Копирование с модификацией ──

    public Envelope WithAttempt(int attempt) =>
        this with { DeliveryAttempt = attempt };

    public Envelope WithHeader(string key, string value)
    {
        var copy = new Dictionary<string, string>(Headers) { [key] = value };
        return this with { Headers = copy.ToFrozenDictionary() };
    }

    public Envelope WithCorrelationId(Guid id) => this with { CorrelationId = id };

    public Envelope WithTenant(string tenantId) => this with { TenantId = tenantId };

    /// <summary>
    /// Проверяет, не истёк ли TTL.
    /// </summary>
    public bool IsExpired(DateTimeOffset now)
    {
        if (TimeToLive is not { } ttl) return false;
        return now - SentAt > ttl;
    }

    /// <summary>
    /// Возвращает true, если сообщение отложено и время доставки ещё не наступило.
    /// </summary>
    public bool IsDeferred(DateTimeOffset now)
    {
        if (DeliverAt is not { } at) return false;
        return now < at;
    }

    /// <summary>
    /// Возвращает true, если дедлайн прошёл.
    /// </summary>
    public bool IsOverdue(DateTimeOffset now)
    {
        if (Deadline is not { } d) return false;
        return now > d;
    }
}
```

---

## AvtoBus.Core/SystemMessage.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Системные/служебные сообщения шины — не пользовательские.
/// </summary>
public static class SystemMessages
{
    public const string SagaSlaViolation = "avtobus.system.saga-sla-violation";
    public const string OutboxRelayError = "avtobus.system.outbox-relay-error";
    public const string Heartbeat = "avtobus.system.heartbeat";
    public const string CircuitBreakerOpened = "avtobus.system.breaker-opened";
    public const string CircuitBreakerClosed = "avtobus.system.breaker-closed";
    public const string DeadLettered = "avtobus.system.dead-lettered";
    public const string ChaosInjected = "avtobus.system.chaos";
    public const string TrafficAnomaly = "avtobus.system.anomaly";
    public const string TopologyChanged = "avtobus.system.topology";
}
```

---

## AvtoBus.Core/Result.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Результат обработки хендлера, который определяет судьбу сообщения:
/// Ok — успех и потенциальные каскадные публикации;
/// Reject — бизнес-отказ без ретрая;
/// Retry — транзиентная ошибка, ретраить.
/// </summary>
public readonly record struct Result
{
    public bool IsOk { get; init; }
    public string? RejectReason { get; init; }
    public IReadOnlyList<object> Cascades { get; init; }

    public static Result Ok(params object[] cascades) => new()
    {
        IsOk = true,
        Cascades = cascades,
        RejectReason = null
    };

    public static Result Reject(string reason) => new()
    {
        IsOk = false,
        RejectReason = reason,
        Cascades = Array.Empty<object>()
    };

    public static implicit operator Result(object[] cascades) => Ok(cascades);
}

/// <summary>
/// Результат обработки с типизированным возвратом.
/// </summary>
public readonly record struct Result<T>
{
    public bool IsOk { get; init; }
    public T? Value { get; init; }
    public string? RejectReason { get; init; }

    public static Result<T> Ok(T value) => new() { IsOk = true, Value = value };
    public static Result<T> Reject(string reason) => new() { IsOk = false, RejectReason = reason };

    public static implicit operator Result<T>(T value) => Ok(value);
}
```

---

## AvtoBus.Core/OutgoingMessages.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Динамический набор исходящих сообщений из хендлера.
/// Позволяет отправлять разные типы сообщений в разные очереди/топики.
/// </summary>
public sealed class OutgoingMessages
{
    private readonly List<OutgoingItem> _items = new();

    public IReadOnlyList<OutgoingItem> Items => _items;

    public OutgoingMessages Send(object message, SendOptions? options = null)
    {
        _items.Add(new OutgoingItem(message, OutgoingKind.Send, options));
        return this;
    }

    public OutgoingMessages Publish(object message, PublishOptions? options = null)
    {
        _items.Add(new OutgoingItem(message, OutgoingKind.Publish, options));
        return this;
    }

    public OutgoingMessages Schedule(object message, TimeSpan delay)
    {
        _items.Add(new OutgoingItem(message, OutgoingKind.Publish,
            new PublishOptions { Delay = delay }));
        return this;
    }

    public OutgoingMessages Schedule(object message, DateTimeOffset at)
    {
        var delay = at - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
        return Schedule(message, delay);
    }

    public OutgoingMessages RespondTo(ConsumeContext context, object reply)
    {
        if (context.Envelope.ReplyTo is not null)
            _items.Add(new OutgoingItem(reply, OutgoingKind.Reply, null));
        return this;
    }

    /// <summary>
    /// Применяет все исходящие сообщения через IBus.
    /// Вызывается из пайплайна после успешной обработки.
    /// </summary>
    public async ValueTask ApplyAsync(IBus bus, ConsumeContext context, CancellationToken ct)
    {
        foreach (var item in _items)
        {
            switch (item.Kind)
            {
                case OutgoingKind.Publish:
                    await bus.Publish(item.Message, item.PublishOptions, ct);
                    break;
                case OutgoingKind.Send:
                    await bus.Send(item.Message, item.SendOptions, ct);
                    break;
                case OutgoingKind.Reply:
                    await context.RespondAsync(item.Message);
                    break;
            }
        }
    }
}

public enum OutgoingKind { Publish, Send, Reply }

public sealed record OutgoingItem(
    object Message,
    OutgoingKind Kind,
    PublishOptions? PublishOptions,
    SendOptions? SendOptions = null);
```

---

## AvtoBus.Core/BusOptions.cs

```csharp
using System.Reflection;
using AvtoBus.Pipeline;

namespace AvtoBus;

/// <summary>
/// Конфигурация шины — fluent API для регистрации.
/// </summary>
public sealed class BusOptions
{
    internal IServiceCollection Services { get; }
    internal BusPipelineBuilder Pipeline { get; } = new();
    internal List<Assembly> ConsumerAssemblies { get; } = new();
    internal List<Type> ConsumerTypes { get; } = new();
    internal string DefaultTransport { get; set; } = "inmemory";
    internal string? DefaultConnectionString { get; set; }
    internal RecoverabilityOptions Recoverability { get; } = new();
    internal List<IRouteConfiguration> Routes { get; } = new();
    internal OutboxOptions? OutboxOptions { get; set; }
    internal InboxOptions? InboxOptions { get; set; }
    internal ChaosOptions? ChaosOptions { get; set; }
    internal List<SagaConfiguration> Sagas { get; } = new();

    public BusOptions(IServiceCollection services) => Services = services;

    /// <summary>
    /// Сканировать сборку на хендлеры (методы Handle/Consume и интерфейсы IConsumer).
    /// </summary>
    public BusOptions AddConsumersFromAssembly(Assembly assembly)
    {
        ConsumerAssemblies.Add(assembly);
        return this;
    }

    /// <summary>
    /// Зарегистрировать конкретный тип хендлера.
    /// </summary>
    public BusOptions AddConsumer<TConsumer>() where TConsumer : class
    {
        ConsumerTypes.Add(typeof(TConsumer));
        return this;
    }

    /// <summary>
    /// Настроить пайплайн middleware.
    /// </summary>
    public BusOptions ConfigurePipeline(Action<BusPipelineBuilder> configure)
    {
        configure(Pipeline);
        return this;
    }

    /// <summary>
    /// Настроить политики восстановления (ретраи, DLQ, игнорирование).
    /// </summary>
    public BusOptions Recoverability(Action<RecoverabilityBuilder> configure)
    {
        configure(new RecoverabilityBuilder(Recoverability));
        return this;
    }

    /// <summary>
    /// Добавить правила маршрутизации.
    /// </summary>
    public BusOptions Routes(Action<RouteBuilder> configure)
    {
        configure(new RouteBuilder(Routes));
        return this;
    }

    /// <summary>
    /// Добавить сагу.
    /// </summary>
    public BusOptions AddSaga<TSaga, TState>()
        where TSaga : Saga<TState>, new()
        where TState : SagaState, new()
    {
        Sagas.Add(new SagaConfiguration(typeof(TSaga), typeof(TState)));
        return this;
    }

    /// <summary>
    /// Добавить сагу с SLA.
    /// </summary>
    public BusOptions AddSaga<TSaga, TState>(Action<SagaBuilder<TSaga, TState>> configure)
        where TSaga : Saga<TState>, new()
        where TState : SagaState, new()
    {
        var cfg = new SagaConfiguration(typeof(TSaga), typeof(TState));
        configure(new SagaBuilder<TSaga, TState>(cfg));
        Sagas.Add(cfg);
        return this;
    }
}
```

---

## AvtoBus.Core/SendOptions.cs / PublishOptions.cs

```csharp
namespace AvtoBus;

public sealed record SendOptions : PublishOptions;

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
    public string? Consumer { get; init; }

    /// <summary>
    /// Создать опции с заголовками из исходящего.
    /// </summary>
    public PublishOptions MergeHeaders(PublishOptions? other)
    {
        if (other is null) return this;
        var merged = new Dictionary<string, string>(Headers ?? new());
        if (other.Headers is not null)
            foreach (var kv in other.Headers)
                merged[kv.Key] = kv.Value;
        return this with { Headers = merged };
    }
}
```

---

## AvtoBus.Core/Metrics.cs

```csharp
using System.Diagnostics.Metrics;

namespace AvtoBus;

/// <summary>
/// Метрики шины — единый точка для всех модулей.
/// </summary>
public static class BusMetrics
{
    public const string MeterName = "AvtoBus";
    public static readonly Meter Meter = new(MeterName);

    // ── Counters ──
    public static readonly Counter<long> PublishCount =
        Meter.CreateCounter<long>("avtobus.publish.count", "messages", "Количество отправленных сообщений");

    public static readonly Counter<long> ConsumeCount =
        Meter.CreateCounter<long>("avtobus.consume.count", "messages", "Количество обработанных сообщений");

    public static readonly Counter<long> ConsumeErrorCount =
        Meter.CreateCounter<long>("avtobus.consume.errors", "messages", "Количество ошибок обработки");

    public static readonly Counter<long> DeadLetteredCount =
        Meter.CreateCounter<long>("avtobus.dead-lettered", "messages", "Количество сообщений в DLQ");

    public static readonly Counter<long> RetryCount =
        Meter.CreateCounter<long>("avtobus.retry", "attempts", "Количество повторных попыток");

    public static readonly Counter<long> InboxDeduped =
        Meter.CreateCounter<long>("avtobus.inbox.deduped", "messages", "Количество отфильтрованных дублей");

    public static readonly Counter<long> SagaStarted =
        Meter.CreateCounter<long>("avtobus.saga.started", "instances", "Новые инстансы саг");

    public static readonly Counter<long> SagaCompleted =
        Meter.CreateCounter<long>("avtobus.saga.completed", "instances", "Завершённые инстансы саг");

    public static readonly Counter<long> SagaAborted =
        Meter.CreateCounter<long>("avtobus.saga.aborted", "instances", "Отменённые инстансы саг");

    // ── Histograms ──
    public static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>("avtobus.publish.duration", "ms", "Время публикации");

    public static readonly Histogram<double> ConsumeDuration =
        Meter.CreateHistogram<double>("avtobus.consume.duration", "ms", "Время обработки сообщения");

    public static readonly Histogram<double> CriticalTime =
        Meter.CreateHistogram<double>("avtobus.critical.time", "ms",
            "Полное время от создания до обработки");

    // ── Gauges ──
    public static readonly ObservableUpDownCounter<int> OutboxPending =
        Meter.CreateObservableUpDownCounter<int>("avtobus.outbox.pending",
            () => new Measurement<int>(OutboxPendingCount));

    public static readonly ObservableUpDownCounter<int> InboxCount =
        Meter.CreateObservableUpDownCounter<int>("avtobus.inbox.count",
            () => new Measurement<int>(InboxCountValue));

    public static readonly ObservableUpDownCounter<int> QueueDepth =
        Meter.CreateObservableUpDownCounter<int>("avtobus.queue.depth",
            () => new Measurement<int>(QueueDepthValue));

    // ── Internal state (обновляются из фоновых сервисов) ──
    internal static int OutboxPendingCount;
    internal static int InboxCountValue;
    internal static int QueueDepthValue;

    // ── Хелперы ──
    public static void RecordConsume(string messageType, double ms, string status = "ok")
    {
        var tags = new TagList
        {
            { "messaging.system", "avtobus" },
            { "messaging.destination.name", messageType },
            { "status", status }
        };
        ConsumeDuration.Record(ms, tags);
        ConsumeCount.Add(1, tags);
    }

    public static void RecordCriticalTime(double ms)
    {
        CriticalTime.Record(ms);
    }
}
```

---

## AvtoBus.Core/Diagnostics.cs

```csharp
using System.Diagnostics;

namespace AvtoBus;

/// <summary>
/// Activity Source для OpenTelemetry.
/// </summary>
public static class BusTracing
{
    public static readonly ActivitySource Source = new("AvtoBus");

    public static Activity? StartConsume(Envelope envelope)
    {
        var activity = Source.StartActivity(
            $"handle {envelope.MessageType}",
            ActivityKind.Consumer,
            parentId: envelope.TraceParent);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "avtobus");
            activity.SetTag("messaging.message.id", envelope.MessageId.ToString());
            activity.SetTag("messaging.destination.name", envelope.MessageType);
            activity.SetTag("messaging.operation.type", "process");
            activity.SetTag("avtobus.attempt", envelope.DeliveryAttempt);

            if (envelope.CorrelationId is { } corr)
                activity.SetTag("avtobus.correlation_id", corr.ToString());

            if (envelope.TenantId is { } tenant)
                activity.SetTag("avtobus.tenant_id", tenant);
        }

        return activity;
    }

    public static Activity? StartPublish(string messageType, Envelope envelope)
    {
        var activity = Source.StartActivity(
            $"publish {messageType}",
            ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "avtobus");
            activity.SetTag("messaging.message.id", envelope.MessageId.ToString());
            activity.SetTag("messaging.destination.name", messageType);
            activity.SetTag("messaging.operation.type", "send");

            // Связываем с входящим, если мы внутри обработки
            if (Activity.Current?.ParentId is not null)
                activity.SetTag("avtobus.causation_id", envelope.CausationId?.ToString());
        }

        return activity;
    }
}
```
