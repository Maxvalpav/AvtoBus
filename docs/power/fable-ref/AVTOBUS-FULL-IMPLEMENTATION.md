# AvtoBus: полная архитектура и код реализации

Единый документ: архитектура фреймворка и полный исходный код реализации ядра (MVP-1 уровень: core messaging, outbox/inbox, sagas, retry/dead-letter, in-memory + RabbitMQ транспорты, EF Core durability, hosting, test harness). Код согласован между секциями: все типы, на которые есть ссылки, определены в документе.

Target: .NET 10 / C# 13, ASP.NET Core 10.

---

## Часть I. Архитектура

### 1. Слои

```text
┌────────────────────────────────────────────────────────────┐
│  Application (ASP.NET Core Minimal API / Worker)          │
│      handlers, sagas, contracts                            │
├────────────────────────────────────────────────────────────┤
│  AvtoBus.Hosting          DI, hosted services, health      │
├────────────────────────────────────────────────────────────┤
│  AvtoBus.Core                                              │
│   ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌──────────────┐  │
│   │ Bus API  │ │ Pipeline│ │ Routing  │ │ Saga runtime │  │
│   └──────────┘ └─────────┘ └──────────┘ └──────────────┘  │
│   ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌──────────────┐  │
│   │ Outbox   │ │ Inbox   │ │ Retry    │ │ Scheduler    │  │
│   │dispatcher│ │ dedup   │ │ policies │ │              │  │
│   └──────────┘ └─────────┘ └──────────┘ └──────────────┘  │
├──────────────────────────┬─────────────────────────────────┤
│  Durability stores       │  Transports                     │
│  (EF Core / PostgreSQL)  │  (InMemory / RabbitMQ / Kafka)  │
└──────────────────────────┴─────────────────────────────────┘
```

### 2. Потоки данных

**Отправка команды с outbox:**

```text
HTTP → IAvtoBus.SendAsync
     → RouteTable находит destination
     → сериализация в AvtoEnvelope
     → IOutboxStore.AddAsync (в транзакции приложения)
     → commit
     → OutboxDispatcherService (фон) → IAvtoTransport.SendAsync → mark dispatched
```

**Приём сообщения:**

```text
Transport delivery → TransportReceiverService
     → десериализация envelope
     → InboxDeduplicator.CheckAsync (dedup по MessageId+ConsumerId)
     → HandlerPipeline.InvokeAsync
         → middleware Before
         → handler invoker (generated или reflection fallback)
         → AvtoEffects → materialize (reply/publish/send/schedule → outbox)
         → inbox mark consumed
     → ack / retry / dead-letter согласно RetryPolicy
```

**Saga:**

```text
Message → SagaHandlerInvoker
     → SagaCorrelation.GetCorrelationId(message)
     → ISagaStore.LoadAsync
     → new saga (Start) или existing (Handle)
     → effects materialized
     → ISagaStore.SaveAsync (optimistic concurrency: version++)
```

### 3. Ключевые решения

| Решение | Обоснование |
| --- | --- |
| Routing по concrete type | schema identity, source generation, AOT |
| Effects как return values | тестируемость, transactional outbox by design |
| Handler invoker абстракция | MVP работает на reflection, прод — на source generation, контракт один |
| Capability-based transports | Kafka ≠ RabbitMQ, нельзя прятать различия |
| Outbox state machine | Pending → Dispatching → Dispatched / Failed |
| Inbox PK (MessageId, ConsumerId) | ровно-один-раз effect при atomic commit |

### 4. Структура solution

```text
src/
├── AvtoBus.Abstractions/        # контракты, ноль зависимостей
├── AvtoBus.Core/                # runtime
├── AvtoBus.Transport.InMemory/  # тесты и local queues
├── AvtoBus.Transport.RabbitMQ/  # RabbitMQ.Client 7.x
├── AvtoBus.Durability.EFCore/   # outbox/inbox/saga на EF Core
├── AvtoBus.Hosting/             # DI + hosted services
└── AvtoBus.Testing/             # test harness
```

---

## Часть II. AvtoBus.Abstractions

### Файл: Messages.cs

```csharp
namespace AvtoBus.Abstractions;

/// <summary>Базовый маркер сообщения AvtoBus со schema identity.</summary>
public interface IAvtoMessage
{
    static abstract string SchemaName { get; }
    static abstract int SchemaVersion { get; }
}

/// <summary>Команда: ровно один логический владелец.</summary>
public interface ICommand : IAvtoMessage;

/// <summary>Команда с типизированным ответом.</summary>
public interface ICommand<TReply> : ICommand;

/// <summary>Событие: ноль или больше подписчиков.</summary>
public interface IEvent : IAvtoMessage;

/// <summary>Запрос read-модели без побочных эффектов.</summary>
public interface IQuery<TReply> : IAvtoMessage;

/// <summary>Сообщение с ключом партиционирования для ordered обработки.</summary>
public interface IPartitionedMessage
{
    string PartitionKey { get; }
}
```

### Файл: AvtoEnvelope.cs

```csharp
namespace AvtoBus.Abstractions;

/// <summary>
/// Transport-независимый конверт. Payload сериализован в Body,
/// десериализованный объект кэшируется в Message.
/// </summary>
public sealed class AvtoEnvelope
{
    public required Guid MessageId { get; init; }
    public required string MessageType { get; init; }
    public required string SchemaName { get; init; }
    public required int SchemaVersion { get; init; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? ConversationId { get; set; }
    public string? TenantId { get; set; }
    public string? PartitionKey { get; init; }
    public string? ReplyTo { get; set; }
    public string? TraceParent { get; set; }
    public required DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? NotBefore { get; set; }
    public DateTimeOffset? ExpiresAt { get; set; }
    public string ContentType { get; init; } = "application/json";
    public Dictionary<string, string> Headers { get; init; } = new();
    public required byte[] Body { get; init; }

    /// <summary>Десериализованное сообщение (заполняется pipeline).</summary>
    public object? Message { get; set; }

    public int DeliveryAttempt { get; set; }
}
```

### Файл: AvtoEffects.cs

```csharp
using System.Collections;

namespace AvtoBus.Abstractions;

public abstract record AvtoEffect;

public sealed record PublishEffect(object Event) : AvtoEffect;
public sealed record SendEffect(object Command) : AvtoEffect;
public sealed record ReplyEffect(object Reply) : AvtoEffect;
public sealed record ScheduleEffect(object Message, TimeSpan Delay) : AvtoEffect;
public sealed record CompleteSagaEffect : AvtoEffect;

/// <summary>Иммутабельный набор эффектов, возвращаемых handler'ом.</summary>
public sealed class AvtoEffects : IReadOnlyList<AvtoEffect>
{
    public static readonly AvtoEffects None = new([]);

    private readonly AvtoEffect[] _effects;

    private AvtoEffects(AvtoEffect[] effects) => _effects = effects;

    public int Count => _effects.Length;
    public AvtoEffect this[int index] => _effects[index];
    public IEnumerator<AvtoEffect> GetEnumerator()
        => ((IEnumerable<AvtoEffect>)_effects).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public static AvtoEffects Publish(object @event) => new([new PublishEffect(@event)]);
    public static AvtoEffects Send(object command) => new([new SendEffect(command)]);
    public static AvtoEffects Reply(object reply) => new([new ReplyEffect(reply)]);
    public static AvtoEffects Schedule(object message, TimeSpan delay)
        => new([new ScheduleEffect(message, delay)]);
    public static AvtoEffects CompleteSaga() => new([new CompleteSagaEffect()]);

    public static AvtoEffects All(params AvtoEffects[] batches)
    {
        var total = 0;
        foreach (var b in batches) total += b.Count;
        var merged = new AvtoEffect[total];
        var i = 0;
        foreach (var b in batches)
        {
            b._effects.CopyTo(merged, i);
            i += b.Count;
        }
        return new AvtoEffects(merged);
    }
}
```

### Файл: IAvtoBus.cs

```csharp
namespace AvtoBus.Abstractions;

public interface IAvtoBus
{
    /// <summary>Команда одному владельцу. Ошибка если маршрут не найден.</summary>
    ValueTask SendAsync(object command, CancellationToken ct = default);

    /// <summary>Событие всем подписчикам. Молча успешно, если подписчиков нет.</summary>
    ValueTask PublishAsync(object @event, CancellationToken ct = default);

    /// <summary>In-process вызов с ожиданием типизированного ответа.</summary>
    ValueTask<TReply> InvokeAsync<TReply>(object message, CancellationToken ct = default);

    /// <summary>Отложенная доставка.</summary>
    ValueTask ScheduleAsync(object message, TimeSpan delay, CancellationToken ct = default);
}
```

### Файл: Handlers.cs

```csharp
namespace AvtoBus.Abstractions;

/// <summary>Результат выполнения handler.</summary>
public sealed record AvtoHandlerOutcome(
    AvtoHandlerStatus Status,
    AvtoEffects Effects,
    string? StopReason = null)
{
    public static AvtoHandlerOutcome Success(AvtoEffects effects) => new(AvtoHandlerStatus.Success, effects);
    public static AvtoHandlerOutcome Stopped(string reason) => new(AvtoHandlerStatus.Stopped, AvtoEffects.None, reason);
}

public enum AvtoHandlerStatus { Success, Stopped }

/// <summary>Контекст вызова handler.</summary>
public sealed class AvtoInvocationContext
{
    public required AvtoEnvelope Envelope { get; init; }
    public required IServiceProvider Services { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}

/// <summary>
/// Контракт исполнимого handler. Source generator создаёт реализации;
/// MVP fallback — ReflectionHandlerInvoker.
/// </summary>
public interface IAvtoHandlerInvoker
{
    string HandlerName { get; }
    Type MessageType { get; }
    ValueTask<AvtoHandlerOutcome> InvokeAsync(AvtoInvocationContext context);
}

public enum HandlerContinuation { Continue, Stop }

public sealed record ValidationResult(bool IsValid, string? Error)
{
    public static readonly ValidationResult Valid = new(true, null);
    public static ValidationResult Invalid(string error) => new(false, error);
}
```

### Файл: Transport.cs

```csharp
namespace AvtoBus.Abstractions;

[Flags]
public enum AvtoTransportCapabilities
{
    None = 0,
    Queues = 1,
    Topics = 2,
    ConsumerGroups = 4,
    PartitionOrdering = 8,
    DelayedDelivery = 16,
    NativeDeadLetter = 32,
    Replay = 64,
}

/// <summary>Исходящий пакет: envelope + адрес назначения.</summary>
public sealed record AvtoOutgoing(AvtoEnvelope Envelope, string Destination);

/// <summary>Callback обработки входящего сообщения. Возвращает true = ack.</summary>
public delegate Task<bool> AvtoDeliveryHandler(AvtoEnvelope envelope, CancellationToken ct);

public interface IAvtoTransport : IAsyncDisposable
{
    string Name { get; }
    AvtoTransportCapabilities Capabilities { get; }

    ValueTask SendAsync(AvtoOutgoing outgoing, CancellationToken ct);

    /// <summary>Подписка на endpoint (queue/topic). Возвращает handle для остановки.</summary>
    ValueTask<IAsyncDisposable> SubscribeAsync(
        string endpoint,
        AvtoDeliveryHandler handler,
        CancellationToken ct);
}
```

### Файл: Durability.cs

```csharp
namespace AvtoBus.Abstractions;

public enum OutboxState { Pending, Dispatching, Dispatched, Failed }

public sealed class OutboxRecord
{
    public required Guid Id { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public required string Destination { get; init; }
    public required string Transport { get; init; }
    public OutboxState State { get; set; } = OutboxState.Pending;
    public int AttemptCount { get; set; }
    public DateTimeOffset NextAttemptAt { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DispatchedAt { get; set; }
}

public interface IOutboxStore
{
    ValueTask AddAsync(IReadOnlyList<OutboxRecord> records, CancellationToken ct);

    /// <summary>Забрать и залочить batch pending-записей (skip locked семантика).</summary>
    ValueTask<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(
        int batchSize, string claimedBy, TimeSpan lockDuration, CancellationToken ct);

    ValueTask MarkDispatchedAsync(IReadOnlyList<Guid> ids, CancellationToken ct);
    ValueTask MarkFailedAttemptAsync(Guid id, string error, DateTimeOffset nextAttempt, CancellationToken ct);
    ValueTask MoveToDeadLetterAsync(Guid id, string reason, CancellationToken ct);
}

public interface IInboxStore
{
    /// <summary>true = сообщение уже обработано этим consumer.</summary>
    ValueTask<bool> IsDuplicateAsync(Guid messageId, string consumerId, CancellationToken ct);
    ValueTask MarkConsumedAsync(Guid messageId, string consumerId, string messageType, CancellationToken ct);
}

public sealed class SagaRecord
{
    public required string Id { get; init; }              // "{SagaType}:{CorrelationId}"
    public required string SagaType { get; init; }
    public required string CorrelationId { get; init; }
    public required byte[] State { get; set; }
    public long Version { get; set; }
    public string Status { get; set; } = "Active";
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public sealed class SagaConcurrencyException(string sagaId)
    : Exception($"Optimistic concurrency conflict on saga '{sagaId}'.");

public interface ISagaStore
{
    ValueTask<SagaRecord?> LoadAsync(string sagaType, string correlationId, CancellationToken ct);
    /// <summary>Insert при Version==0, иначе update с проверкой версии. Кидает SagaConcurrencyException.</summary>
    ValueTask SaveAsync(SagaRecord record, CancellationToken ct);
    ValueTask CompleteAsync(string sagaId, CancellationToken ct);
}

public sealed class DeadLetterRecord
{
    public required Guid Id { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public required string Reason { get; init; }
    public required string Endpoint { get; init; }
    public string? ExceptionType { get; init; }
    public int AttemptCount { get; init; }
    public DateTimeOffset FailedAt { get; init; } = DateTimeOffset.UtcNow;
}

public interface IDeadLetterStore
{
    ValueTask AddAsync(DeadLetterRecord record, CancellationToken ct);
    ValueTask<IReadOnlyList<DeadLetterRecord>> ListAsync(int limit, CancellationToken ct);
    ValueTask<DeadLetterRecord?> GetAsync(Guid id, CancellationToken ct);
    ValueTask RemoveAsync(Guid id, CancellationToken ct);
}
```

### Файл: Serialization.cs

```csharp
namespace AvtoBus.Abstractions;

public interface IAvtoSerializer
{
    byte[] Serialize(object message);
    object Deserialize(byte[] body, Type messageType);
}

/// <summary>Реестр типов сообщений: schema name ↔ CLR type.</summary>
public interface IAvtoMessageTypeRegistry
{
    void Register(Type messageType, string schemaName, int schemaVersion);
    Type? Resolve(string schemaName);
    (string SchemaName, int SchemaVersion) Describe(Type messageType);
}
```

### Файл: Saga.cs

```csharp
namespace AvtoBus.Abstractions;

/// <summary>Базовый класс saga. Состояние — свойства наследника.</summary>
public abstract class AvtoSaga
{
    public string? SagaId { get; internal set; }
    public long Version { get; internal set; }
}

/// <summary>Дескриптор saga: корреляция + вызов. Реализуется генератором или reflection.</summary>
public interface IAvtoSagaDescriptor
{
    string SagaType { get; }
    Type SagaClrType { get; }
    IReadOnlyList<Type> MessageTypes { get; }
    bool CanStart(Type messageType);
    string GetCorrelationId(object message);
    AvtoEffects InvokeStart(AvtoSaga saga, object message);
    AvtoEffects InvokeHandle(AvtoSaga saga, object message);
}
```

---

## Часть III. AvtoBus.Core

### Файл: Serialization/SystemTextJsonSerializer.cs

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Serialization;

public sealed class SystemTextJsonSerializer : IAvtoSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
    }

    public byte[] Serialize(object message)
        => JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), _options);

    public object Deserialize(byte[] body, Type messageType)
        => JsonSerializer.Deserialize(body, messageType, _options)
           ?? throw new InvalidOperationException($"Deserialization of {messageType.Name} returned null.");
}
```

### Файл: Serialization/MessageTypeRegistry.cs

```csharp
using System.Collections.Concurrent;
using System.Reflection;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Serialization;

public sealed class MessageTypeRegistry : IAvtoMessageTypeRegistry
{
    private readonly ConcurrentDictionary<string, Type> _bySchema = new();
    private readonly ConcurrentDictionary<Type, (string, int)> _byType = new();

    public void Register(Type messageType, string schemaName, int schemaVersion)
    {
        _bySchema[schemaName] = messageType;
        _byType[messageType] = (schemaName, schemaVersion);
    }

    public Type? Resolve(string schemaName)
        => _bySchema.TryGetValue(schemaName, out var t) ? t : null;

    public (string SchemaName, int SchemaVersion) Describe(Type messageType)
    {
        if (_byType.TryGetValue(messageType, out var known))
            return known;

        // Fallback: статические члены IAvtoMessage через reflection (MVP).
        var name = messageType.GetProperty("SchemaName",
            BindingFlags.Public | BindingFlags.Static)?.GetValue(null) as string;
        var version = messageType.GetProperty("SchemaVersion",
            BindingFlags.Public | BindingFlags.Static)?.GetValue(null) as int?;

        var result = (name ?? messageType.FullName!, version ?? 1);
        Register(messageType, result.Item1, result.Item2);
        return result;
    }
}
```

### Файл: Routing/RouteTable.cs

```csharp
using System.Collections.Concurrent;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Routing;

public enum RouteKind { Command, Event, Local }

public sealed record RouteEntry(
    Type MessageType,
    RouteKind Kind,
    string Transport,
    string Destination);

public sealed class RouteTable
{
    private readonly ConcurrentDictionary<Type, List<RouteEntry>> _routes = new();

    public void Add(RouteEntry entry)
    {
        var list = _routes.GetOrAdd(entry.MessageType, _ => []);
        lock (list)
        {
            if (entry.Kind == RouteKind.Command &&
                list.Any(e => e.Kind == RouteKind.Command))
            {
                throw new InvalidOperationException(
                    $"Command {entry.MessageType.Name} already has an owner route. " +
                    "Commands must have exactly one destination.");
            }
            list.Add(entry);
        }
    }

    public IReadOnlyList<RouteEntry> RoutesFor(Type messageType)
        => _routes.TryGetValue(messageType, out var list) ? list : [];

    public IEnumerable<RouteEntry> All => _routes.Values.SelectMany(v => v);
}
```

### Файл: Handlers/HandlerRegistry.cs

```csharp
using System.Collections.Concurrent;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Handlers;

public sealed class HandlerRegistry
{
    private readonly ConcurrentDictionary<Type, List<IAvtoHandlerInvoker>> _handlers = new();

    public void Add(IAvtoHandlerInvoker invoker)
        => _handlers.GetOrAdd(invoker.MessageType, _ => []).Add(invoker);

    public IReadOnlyList<IAvtoHandlerInvoker> For(Type messageType)
        => _handlers.TryGetValue(messageType, out var list) ? list : [];

    public IEnumerable<IAvtoHandlerInvoker> All => _handlers.Values.SelectMany(v => v);
}
```

### Файл: Handlers/ReflectionHandlerInvoker.cs

MVP-fallback. В production этот класс заменяется сгенерированными инвокерами с тем же контрактом `IAvtoHandlerInvoker`.

```csharp
using System.Reflection;
using AvtoBus.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Core.Handlers;

/// <summary>
/// Reflection-инвокер: находит статический метод Handle, резолвит зависимости
/// по параметрам, маппит результат в effects. Контракт идентичен генерируемому коду.
/// </summary>
public sealed class ReflectionHandlerInvoker : IAvtoHandlerInvoker
{
    private readonly MethodInfo _handleMethod;
    private readonly MethodInfo? _validateMethod;
    private readonly ParameterInfo[] _parameters;

    public string HandlerName { get; }
    public Type MessageType { get; }

    public ReflectionHandlerInvoker(Type handlerClass, MethodInfo handleMethod)
    {
        _handleMethod = handleMethod;
        _parameters = handleMethod.GetParameters();
        MessageType = _parameters[0].ParameterType;
        HandlerName = handlerClass.Name;
        _validateMethod = handlerClass.GetMethod("Validate",
            BindingFlags.Public | BindingFlags.Static, [MessageType]);
    }

    public static IEnumerable<ReflectionHandlerInvoker> Discover(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (!type.IsClass || type.IsNested) continue;
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.Name is not ("Handle" or "HandleAsync")) continue;
                var ps = method.GetParameters();
                if (ps.Length == 0) continue;
                var msgType = ps[0].ParameterType;
                if (!msgType.IsClass || msgType == typeof(object)) continue;
                yield return new ReflectionHandlerInvoker(type, method);
            }
        }
    }

    public async ValueTask<AvtoHandlerOutcome> InvokeAsync(AvtoInvocationContext context)
    {
        var message = context.Envelope.Message
            ?? throw new InvalidOperationException("Envelope.Message not deserialized.");

        // Validate phase
        if (_validateMethod is not null)
        {
            var validation = (ValidationResult)_validateMethod.Invoke(null, [message])!;
            if (!validation.IsValid)
                return AvtoHandlerOutcome.Stopped(validation.Error ?? "validation failed");
        }

        // Resolve args: [0] = message, остальные из DI, CancellationToken отдельно
        var args = new object?[_parameters.Length];
        args[0] = message;
        for (var i = 1; i < _parameters.Length; i++)
        {
            var pt = _parameters[i].ParameterType;
            args[i] = pt == typeof(CancellationToken)
                ? context.CancellationToken
                : context.Services.GetRequiredService(pt);
        }

        var result = _handleMethod.Invoke(null, args);
        var value = await UnwrapAsync(result);
        return AvtoHandlerOutcome.Success(MapEffects(value));
    }

    private static async ValueTask<object?> UnwrapAsync(object? result)
    {
        switch (result)
        {
            case null:
                return null;
            case Task task:
                await task.ConfigureAwait(false);
                var taskType = task.GetType();
                return taskType.IsGenericType
                    ? taskType.GetProperty("Result")!.GetValue(task)
                    : null;
            case ValueTask vt:
                await vt.ConfigureAwait(false);
                return null;
            default:
                var rt = result.GetType();
                if (rt.IsGenericType && rt.GetGenericTypeDefinition() == typeof(ValueTask<>))
                {
                    var asTask = (Task)rt.GetMethod("AsTask")!.Invoke(result, null)!;
                    await asTask.ConfigureAwait(false);
                    return asTask.GetType().GetProperty("Result")!.GetValue(asTask);
                }
                return result;
        }
    }

    /// <summary>Маппинг возврата: AvtoEffects → как есть; tuple → по типам полей; одиночное значение → Reply|Publish.</summary>
    private static AvtoEffects MapEffects(object? value)
    {
        switch (value)
        {
            case null:
                return AvtoEffects.None;
            case AvtoEffects effects:
                return effects;
        }

        var type = value.GetType();
        if (type.FullName!.StartsWith("System.ValueTuple", StringComparison.Ordinal))
        {
            var batches = new List<AvtoEffects>();
            foreach (var field in type.GetFields())
            {
                var item = field.GetValue(value);
                if (item is not null)
                    batches.Add(MapSingle(item));
            }
            return AvtoEffects.All([.. batches]);
        }

        return MapSingle(value);
    }

    private static AvtoEffects MapSingle(object item) =>
        item switch
        {
            IEvent => AvtoEffects.Publish(item),
            ICommand => AvtoEffects.Send(item),
            _ => AvtoEffects.Reply(item),   // не-message тип = ответ вызывающему
        };
}
```

### Файл: Pipeline/EnvelopeFactory.cs

```csharp
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Pipeline;

public sealed class EnvelopeFactory(
    IAvtoSerializer serializer,
    IAvtoMessageTypeRegistry registry,
    TimeProvider clock)
{
    public AvtoEnvelope Create(object message, AvtoEnvelope? parent = null)
    {
        var type = message.GetType();
        var (schemaName, schemaVersion) = registry.Describe(type);

        var envelope = new AvtoEnvelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = type.FullName!,
            SchemaName = schemaName,
            SchemaVersion = schemaVersion,
            PartitionKey = (message as IPartitionedMessage)?.PartitionKey,
            CreatedAt = clock.GetUtcNow(),
            Body = serializer.Serialize(message),
            Message = message,
        };

        if (parent is not null)
        {
            envelope.CorrelationId = parent.CorrelationId ?? parent.MessageId.ToString("N");
            envelope.CausationId = parent.MessageId.ToString("N");
            envelope.ConversationId = parent.ConversationId;
            envelope.TenantId = parent.TenantId;
            envelope.TraceParent = parent.TraceParent;
        }
        else
        {
            envelope.CorrelationId = envelope.MessageId.ToString("N");
        }

        return envelope;
    }
}
```

### Файл: Pipeline/RetryPolicy.cs

```csharp
namespace AvtoBus.Core.Pipeline;

public sealed class RetryPolicy
{
    public int MaxImmediateRetries { get; init; } = 3;
    public int MaxScheduledRetries { get; init; } = 3;
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(5);
    public double JitterFactor { get; init; } = 0.2;

    /// <summary>Не ретраить (сразу dead-letter) для этих типов исключений.</summary>
    public HashSet<Type> NonRetryable { get; init; } = [];

    public int MaxTotalAttempts => MaxImmediateRetries + MaxScheduledRetries + 1;

    public bool IsRetryable(Exception ex)
        => !NonRetryable.Any(t => t.IsInstanceOfType(ex));

    public TimeSpan DelayFor(int attempt)
    {
        var exp = Math.Pow(2, Math.Min(attempt, 10));
        var delay = TimeSpan.FromMilliseconds(BaseDelay.TotalMilliseconds * exp);
        if (delay > MaxDelay) delay = MaxDelay;
        var jitter = 1 + (Random.Shared.NextDouble() * 2 - 1) * JitterFactor;
        return TimeSpan.FromMilliseconds(delay.TotalMilliseconds * jitter);
    }
}
```

### Файл: Pipeline/Telemetry.cs

```csharp
using System.Diagnostics;
using System.Diagnostics.Metrics;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Pipeline;

public static class AvtoTelemetry
{
    public const string SourceName = "AvtoBus";

    public static readonly ActivitySource ActivitySource = new(SourceName, "1.0.0");
    public static readonly Meter Meter = new(SourceName, "1.0.0");

    public static readonly Counter<long> MessagesTotal =
        Meter.CreateCounter<long>("avtobus_messages_total");
    public static readonly Counter<long> DeadLetterTotal =
        Meter.CreateCounter<long>("avtobus_dead_letter_total");
    public static readonly Counter<long> InboxDuplicateTotal =
        Meter.CreateCounter<long>("avtobus_inbox_duplicate_total");
    public static readonly Histogram<double> HandlerDuration =
        Meter.CreateHistogram<double>("avtobus_handler_duration_seconds");
    public static readonly Counter<long> RetryTotal =
        Meter.CreateCounter<long>("avtobus_retry_total");

    public static Activity? StartConsume(AvtoEnvelope envelope, string endpoint)
    {
        var activity = ActivitySource.StartActivity(
            $"avtobus.process {envelope.SchemaName}", ActivityKind.Consumer);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", "avtobus");
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("messaging.destination.name", endpoint);
            activity.SetTag("messaging.message.id", envelope.MessageId);
            activity.SetTag("avtobus.schema.name", envelope.SchemaName);
            activity.SetTag("avtobus.schema.version", envelope.SchemaVersion);
            if (envelope.CorrelationId is not null)
                activity.SetTag("avtobus.correlation.id", envelope.CorrelationId);
        }
        return activity;
    }
}
```

### Файл: Pipeline/EffectMaterializer.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Core.Routing;

namespace AvtoBus.Core.Pipeline;

/// <summary>
/// Превращает effects в outbox-записи (send/publish/schedule) и reply.
/// Вызывается внутри транзакции handler'а — эффекты попадают в outbox атомарно.
/// </summary>
public sealed class EffectMaterializer(
    RouteTable routes,
    EnvelopeFactory envelopeFactory,
    IOutboxStore outbox,
    TimeProvider clock)
{
    public sealed record MaterializeResult(object? Reply, bool SagaCompleted);

    public async ValueTask<MaterializeResult> MaterializeAsync(
        AvtoEffects effects, AvtoEnvelope causingEnvelope, CancellationToken ct)
    {
        object? reply = null;
        var sagaCompleted = false;
        var outboxRecords = new List<OutboxRecord>();

        foreach (var effect in effects)
        {
            switch (effect)
            {
                case ReplyEffect r:
                    reply = r.Reply;
                    break;

                case CompleteSagaEffect:
                    sagaCompleted = true;
                    break;

                case PublishEffect p:
                    AddRecords(outboxRecords, p.Event, causingEnvelope, delay: null);
                    break;

                case SendEffect s:
                    AddCommandRecord(outboxRecords, s.Command, causingEnvelope, delay: null);
                    break;

                case ScheduleEffect sch:
                    var isCommand = sch.Message is ICommand;
                    if (isCommand)
                        AddCommandRecord(outboxRecords, sch.Message, causingEnvelope, sch.Delay);
                    else
                        AddRecords(outboxRecords, sch.Message, causingEnvelope, sch.Delay);
                    break;
            }
        }

        if (outboxRecords.Count > 0)
            await outbox.AddAsync(outboxRecords, ct);

        return new MaterializeResult(reply, sagaCompleted);
    }

    private void AddRecords(
        List<OutboxRecord> records, object @event, AvtoEnvelope parent, TimeSpan? delay)
    {
        var eventRoutes = routes.RoutesFor(@event.GetType())
            .Where(r => r.Kind is RouteKind.Event or RouteKind.Local);
        foreach (var route in eventRoutes)
            records.Add(CreateRecord(@event, parent, route, delay));
    }

    private void AddCommandRecord(
        List<OutboxRecord> records, object command, AvtoEnvelope parent, TimeSpan? delay)
    {
        var route = routes.RoutesFor(command.GetType())
            .FirstOrDefault(r => r.Kind is RouteKind.Command or RouteKind.Local)
            ?? throw new InvalidOperationException(
                $"No command route for {command.GetType().Name}.");
        records.Add(CreateRecord(command, parent, route, delay));
    }

    private OutboxRecord CreateRecord(
        object message, AvtoEnvelope parent, RouteEntry route, TimeSpan? delay)
    {
        var envelope = envelopeFactory.Create(message, parent);
        if (delay is not null)
            envelope.NotBefore = clock.GetUtcNow() + delay.Value;

        return new OutboxRecord
        {
            Id = Guid.NewGuid(),
            Envelope = envelope,
            Destination = route.Destination,
            Transport = route.Transport,
            NextAttemptAt = envelope.NotBefore ?? clock.GetUtcNow(),
        };
    }
}
```

### Файл: Pipeline/HandlerPipeline.cs

```csharp
using System.Diagnostics;
using AvtoBus.Abstractions;
using AvtoBus.Core.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Core.Pipeline;

public sealed record PipelineResult(bool Handled, object? Reply);

/// <summary>
/// Центральный pipeline обработки входящего envelope:
/// deserialization → inbox dedup → handlers → effects → inbox mark → retry/DLQ.
/// </summary>
public sealed class HandlerPipeline(
    HandlerRegistry handlers,
    SagaRuntime sagaRuntime,
    IAvtoSerializer serializer,
    IAvtoMessageTypeRegistry typeRegistry,
    IServiceScopeFactory scopeFactory,
    RetryPolicy retryPolicy,
    IInboxStore inbox,
    IDeadLetterStore deadLetters,
    ILogger<HandlerPipeline> logger)
{
    public async Task<PipelineResult> ProcessAsync(
        AvtoEnvelope envelope, string endpoint, CancellationToken ct)
    {
        using var activity = AvtoTelemetry.StartConsume(envelope, endpoint);
        var sw = Stopwatch.StartNew();

        // 1. Deserialize
        if (envelope.Message is null)
        {
            var clrType = typeRegistry.Resolve(envelope.SchemaName)
                ?? Type.GetType(envelope.MessageType);
            if (clrType is null)
            {
                await QuarantineAsync(envelope, endpoint, "unknown-message-type", ct);
                return new PipelineResult(false, null);
            }
            envelope.Message = serializer.Deserialize(envelope.Body, clrType);
        }

        var messageType = envelope.Message.GetType();

        // 2. Inbox dedup
        if (await inbox.IsDuplicateAsync(envelope.MessageId, endpoint, ct))
        {
            AvtoTelemetry.InboxDuplicateTotal.Add(1);
            logger.LogDebug("Duplicate {MessageId} on {Endpoint}, skipping.",
                envelope.MessageId, endpoint);
            return new PipelineResult(true, null);
        }

        // 3. Immediate retry loop
        for (var attempt = 1; ; attempt++)
        {
            envelope.DeliveryAttempt = attempt;
            try
            {
                var reply = await ExecuteOnceAsync(envelope, messageType, endpoint, ct);
                AvtoTelemetry.MessagesTotal.Add(1,
                    new KeyValuePair<string, object?>("outcome", "success"),
                    new KeyValuePair<string, object?>("endpoint", endpoint));
                AvtoTelemetry.HandlerDuration.Record(sw.Elapsed.TotalSeconds);
                return new PipelineResult(true, reply);
            }
            catch (Exception ex) when (retryPolicy.IsRetryable(ex)
                                       && attempt <= retryPolicy.MaxImmediateRetries)
            {
                AvtoTelemetry.RetryTotal.Add(1);
                logger.LogWarning(ex,
                    "Handler failed (attempt {Attempt}) for {MessageId}, retrying.",
                    attempt, envelope.MessageId);
                await Task.Delay(retryPolicy.DelayFor(attempt), ct);
            }
            catch (Exception ex)
            {
                AvtoTelemetry.MessagesTotal.Add(1,
                    new KeyValuePair<string, object?>("outcome", "failed"),
                    new KeyValuePair<string, object?>("endpoint", endpoint));
                await DeadLetterAsync(envelope, endpoint, ex, ct);
                return new PipelineResult(true, null); // ack: сообщение ушло в DLQ
            }
        }
    }

    private async Task<object?> ExecuteOnceAsync(
        AvtoEnvelope envelope, Type messageType, string endpoint, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();

        var context = new AvtoInvocationContext
        {
            Envelope = envelope,
            Services = scope.ServiceProvider,
            CancellationToken = ct,
        };

        object? reply = null;
        var materializer = scope.ServiceProvider.GetRequiredService<EffectMaterializer>();

        // Обычные handlers
        foreach (var invoker in handlers.For(messageType))
        {
            var outcome = await invoker.InvokeAsync(context);
            if (outcome.Status == AvtoHandlerStatus.Stopped)
            {
                logger.LogInformation("Handler {Handler} stopped: {Reason}",
                    invoker.HandlerName, outcome.StopReason);
                continue;
            }
            var result = await materializer.MaterializeAsync(outcome.Effects, envelope, ct);
            reply ??= result.Reply;
        }

        // Sagas
        await sagaRuntime.DispatchAsync(envelope, materializer, scope.ServiceProvider, ct);

        // Inbox mark (в том же scope; с EF Core durability — та же транзакция через UnitOfWork)
        await inbox.MarkConsumedAsync(envelope.MessageId, endpoint, envelope.MessageType, ct);

        return reply;
    }

    private async Task DeadLetterAsync(
        AvtoEnvelope envelope, string endpoint, Exception ex, CancellationToken ct)
    {
        AvtoTelemetry.DeadLetterTotal.Add(1,
            new KeyValuePair<string, object?>("reason", ex.GetType().Name));
        logger.LogError(ex, "Message {MessageId} moved to dead letter.", envelope.MessageId);

        await deadLetters.AddAsync(new DeadLetterRecord
        {
            Id = Guid.NewGuid(),
            Envelope = envelope,
            Reason = ex.Message,
            Endpoint = endpoint,
            ExceptionType = ex.GetType().FullName,
            AttemptCount = envelope.DeliveryAttempt,
        }, ct);
    }

    private Task QuarantineAsync(
        AvtoEnvelope envelope, string endpoint, string reason, CancellationToken ct)
        => deadLetters.AddAsync(new DeadLetterRecord
        {
            Id = Guid.NewGuid(),
            Envelope = envelope,
            Reason = reason,
            Endpoint = endpoint,
            AttemptCount = envelope.DeliveryAttempt,
        }, ct).AsTask();
}
```

### Файл: Sagas/SagaRuntime.cs

```csharp
using System.Text.Json;
using AvtoBus.Abstractions;
using AvtoBus.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Core.Sagas;

public sealed class SagaRuntime(
    IReadOnlyList<IAvtoSagaDescriptor> descriptors,
    ILogger<SagaRuntime> logger)
{
    private const int ConcurrencyRetries = 3;

    public async Task DispatchAsync(
        AvtoEnvelope envelope,
        EffectMaterializer materializer,
        IServiceProvider services,
        CancellationToken ct)
    {
        var message = envelope.Message!;
        var messageType = message.GetType();

        foreach (var descriptor in descriptors)
        {
            if (!descriptor.MessageTypes.Contains(messageType))
                continue;

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await DispatchToSagaAsync(descriptor, envelope, message, materializer, services, ct);
                    break;
                }
                catch (SagaConcurrencyException) when (attempt < ConcurrencyRetries)
                {
                    await Task.Delay(Random.Shared.Next(20, 120), ct);
                }
            }
        }
    }

    private async Task DispatchToSagaAsync(
        IAvtoSagaDescriptor descriptor,
        AvtoEnvelope envelope,
        object message,
        EffectMaterializer materializer,
        IServiceProvider services,
        CancellationToken ct)
    {
        var store = services.GetRequiredService<ISagaStore>();
        var correlationId = descriptor.GetCorrelationId(message);
        var record = await store.LoadAsync(descriptor.SagaType, correlationId, ct);

        AvtoSaga saga;
        AvtoEffects effects;

        if (record is null)
        {
            if (!descriptor.CanStart(message.GetType()))
            {
                logger.LogWarning(
                    "Saga {SagaType} not found for correlation {CorrelationId} and message {MessageType} cannot start it.",
                    descriptor.SagaType, correlationId, message.GetType().Name);
                return;
            }

            saga = (AvtoSaga)Activator.CreateInstance(descriptor.SagaClrType)!;
            saga.SagaId = $"{descriptor.SagaType}:{correlationId}";
            effects = descriptor.InvokeStart(saga, message);

            record = new SagaRecord
            {
                Id = saga.SagaId,
                SagaType = descriptor.SagaType,
                CorrelationId = correlationId,
                State = JsonSerializer.SerializeToUtf8Bytes(saga, descriptor.SagaClrType),
                Version = 0,
            };
        }
        else
        {
            saga = (AvtoSaga)JsonSerializer.Deserialize(record.State, descriptor.SagaClrType)!;
            saga.SagaId = record.Id;
            saga.Version = record.Version;
            effects = descriptor.InvokeHandle(saga, message);
            record.State = JsonSerializer.SerializeToUtf8Bytes(saga, descriptor.SagaClrType);
        }

        var result = await materializer.MaterializeAsync(effects, envelope, ct);

        if (result.SagaCompleted)
        {
            record.Status = "Completed";
            await store.SaveAsync(record, ct);
            await store.CompleteAsync(record.Id, ct);
        }
        else
        {
            await store.SaveAsync(record, ct);
        }
    }
}
```

### Файл: Sagas/ReflectionSagaDescriptor.cs

```csharp
using System.Reflection;
using AvtoBus.Abstractions;

namespace AvtoBus.Core.Sagas;

/// <summary>MVP-descriptor через reflection. Production — source generation с тем же контрактом.</summary>
public sealed class ReflectionSagaDescriptor : IAvtoSagaDescriptor
{
    private readonly Dictionary<Type, MethodInfo> _correlateMethods = new();
    private readonly Dictionary<Type, MethodInfo> _startMethods = new();
    private readonly Dictionary<Type, MethodInfo> _handleMethods = new();

    public string SagaType { get; }
    public Type SagaClrType { get; }
    public IReadOnlyList<Type> MessageTypes { get; }

    public ReflectionSagaDescriptor(Type sagaClrType)
    {
        SagaClrType = sagaClrType;
        SagaType = sagaClrType.Name;

        foreach (var m in sagaClrType.GetMethods(BindingFlags.Public | BindingFlags.Static))
        {
            if (m.Name != "Correlate") continue;
            var msgType = m.GetParameters()[0].ParameterType;
            _correlateMethods[msgType] = m;
        }

        foreach (var m in sagaClrType.GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            if (m.GetParameters().Length != 1) continue;
            var msgType = m.GetParameters()[0].ParameterType;
            if (!_correlateMethods.ContainsKey(msgType)) continue;

            if (m.Name == "Start") _startMethods[msgType] = m;
            else if (m.Name == "Handle") _handleMethods[msgType] = m;
        }

        MessageTypes = [.. _correlateMethods.Keys];
    }

    public bool CanStart(Type messageType) => _startMethods.ContainsKey(messageType);

    public string GetCorrelationId(object message)
        => _correlateMethods[message.GetType()].Invoke(null, [message])!.ToString()!;

    public AvtoEffects InvokeStart(AvtoSaga saga, object message)
        => (AvtoEffects)_startMethods[message.GetType()].Invoke(saga, [message])!;

    public AvtoEffects InvokeHandle(AvtoSaga saga, object message)
        => _handleMethods.TryGetValue(message.GetType(), out var m)
            ? (AvtoEffects)m.Invoke(saga, [message])!
            : _startMethods.TryGetValue(message.GetType(), out var s)
                ? (AvtoEffects)s.Invoke(saga, [message])!
                : AvtoEffects.None;
}
```

### Файл: Bus/AvtoBusClient.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Core.Pipeline;
using AvtoBus.Core.Routing;

namespace AvtoBus.Core.Bus;

public sealed class AvtoBusClient(
    RouteTable routes,
    EnvelopeFactory envelopeFactory,
    IOutboxStore outbox,
    HandlerPipeline pipeline,
    TimeProvider clock) : IAvtoBus
{
    public async ValueTask SendAsync(object command, CancellationToken ct = default)
    {
        var route = routes.RoutesFor(command.GetType())
            .FirstOrDefault(r => r.Kind is RouteKind.Command or RouteKind.Local)
            ?? throw new InvalidOperationException(
                $"No route for command {command.GetType().Name}. Configure routing.");

        var envelope = envelopeFactory.Create(command);
        await outbox.AddAsync([new OutboxRecord
        {
            Id = Guid.NewGuid(),
            Envelope = envelope,
            Destination = route.Destination,
            Transport = route.Transport,
            NextAttemptAt = clock.GetUtcNow(),
        }], ct);
    }

    public async ValueTask PublishAsync(object @event, CancellationToken ct = default)
    {
        var eventRoutes = routes.RoutesFor(@event.GetType())
            .Where(r => r.Kind is RouteKind.Event or RouteKind.Local)
            .ToList();

        if (eventRoutes.Count == 0)
            return; // publish без подписчиков — тихо ок

        var records = new List<OutboxRecord>(eventRoutes.Count);
        foreach (var route in eventRoutes)
        {
            var envelope = envelopeFactory.Create(@event);
            records.Add(new OutboxRecord
            {
                Id = Guid.NewGuid(),
                Envelope = envelope,
                Destination = route.Destination,
                Transport = route.Transport,
                NextAttemptAt = clock.GetUtcNow(),
            });
        }
        await outbox.AddAsync(records, ct);
    }

    public async ValueTask<TReply> InvokeAsync<TReply>(object message, CancellationToken ct = default)
    {
        // In-process: сразу через pipeline, без брокера.
        var envelope = envelopeFactory.Create(message);
        var result = await pipeline.ProcessAsync(envelope, endpoint: "local", ct);

        if (result.Reply is TReply typed)
            return typed;

        throw new InvalidOperationException(
            $"Handler for {message.GetType().Name} did not produce a reply of type {typeof(TReply).Name}.");
    }

    public async ValueTask ScheduleAsync(object message, TimeSpan delay, CancellationToken ct = default)
    {
        var isCommand = message is ICommand;
        var route = routes.RoutesFor(message.GetType())
            .FirstOrDefault(r => isCommand
                ? r.Kind is RouteKind.Command or RouteKind.Local
                : r.Kind is RouteKind.Event or RouteKind.Local)
            ?? throw new InvalidOperationException(
                $"No route for scheduled message {message.GetType().Name}.");

        var envelope = envelopeFactory.Create(message);
        envelope.NotBefore = clock.GetUtcNow() + delay;

        await outbox.AddAsync([new OutboxRecord
        {
            Id = Guid.NewGuid(),
            Envelope = envelope,
            Destination = route.Destination,
            Transport = route.Transport,
            NextAttemptAt = envelope.NotBefore.Value,
        }], ct);
    }
}
```

### Файл: Dispatch/OutboxDispatcherService.cs

```csharp
using AvtoBus.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Core.Dispatch;

public sealed class OutboxDispatcherOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDispatchAttempts { get; set; } = 10;
}

public sealed class OutboxDispatcherService(
    IOutboxStore outbox,
    IReadOnlyDictionary<string, IAvtoTransport> transports,
    OutboxDispatcherOptions options,
    TimeProvider clock,
    ILogger<OutboxDispatcherService> logger) : BackgroundService
{
    private readonly string _instanceId = $"{Environment.MachineName}:{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox dispatcher {Instance} started.", _instanceId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dispatched = await DispatchBatchAsync(stoppingToken);
                if (dispatched == 0)
                    await Task.Delay(options.PollingInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox dispatcher iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task<int> DispatchBatchAsync(CancellationToken ct)
    {
        var batch = await outbox.ClaimBatchAsync(
            options.BatchSize, _instanceId, options.LockDuration, ct);
        if (batch.Count == 0) return 0;

        var succeeded = new List<Guid>(batch.Count);

        foreach (var record in batch)
        {
            if (!transports.TryGetValue(record.Transport, out var transport))
            {
                await outbox.MoveToDeadLetterAsync(
                    record.Id, $"unknown transport '{record.Transport}'", ct);
                continue;
            }

            try
            {
                await transport.SendAsync(
                    new AvtoOutgoing(record.Envelope, record.Destination), ct);
                succeeded.Add(record.Id);
            }
            catch (Exception ex)
            {
                var attempt = record.AttemptCount + 1;
                if (attempt >= options.MaxDispatchAttempts)
                {
                    await outbox.MoveToDeadLetterAsync(record.Id, ex.Message, ct);
                    logger.LogError(ex,
                        "Outbox record {Id} dead-lettered after {Attempts} attempts.",
                        record.Id, attempt);
                }
                else
                {
                    var next = clock.GetUtcNow() + TimeSpan.FromSeconds(Math.Pow(2, attempt));
                    await outbox.MarkFailedAttemptAsync(record.Id, ex.Message, next, ct);
                }
            }
        }

        if (succeeded.Count > 0)
            await outbox.MarkDispatchedAsync(succeeded, ct);

        return succeeded.Count;
    }
}
```

### Файл: Dispatch/TransportReceiverService.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Core.Pipeline;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Core.Dispatch;

public sealed record ReceiveEndpoint(string Transport, string Endpoint);

public sealed class TransportReceiverService(
    IReadOnlyList<ReceiveEndpoint> endpoints,
    IReadOnlyDictionary<string, IAvtoTransport> transports,
    HandlerPipeline pipeline,
    ILogger<TransportReceiverService> logger) : BackgroundService
{
    private readonly List<IAsyncDisposable> _subscriptions = [];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var endpoint in endpoints)
        {
            if (!transports.TryGetValue(endpoint.Transport, out var transport))
            {
                logger.LogError("Unknown transport '{Transport}' for endpoint '{Endpoint}'.",
                    endpoint.Transport, endpoint.Endpoint);
                continue;
            }

            var subscription = await transport.SubscribeAsync(
                endpoint.Endpoint,
                (envelope, ct) => HandleAsync(envelope, endpoint.Endpoint, ct),
                stoppingToken);

            _subscriptions.Add(subscription);
            logger.LogInformation("Listening on {Transport}/{Endpoint}.",
                endpoint.Transport, endpoint.Endpoint);
        }

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken)
            .ContinueWith(_ => { }, TaskScheduler.Default);
    }

    private async Task<bool> HandleAsync(AvtoEnvelope envelope, string endpoint, CancellationToken ct)
    {
        var result = await pipeline.ProcessAsync(envelope, endpoint, ct);
        return result.Handled; // true = ack, false = redeliver
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var s in _subscriptions)
            await s.DisposeAsync();
        await base.StopAsync(cancellationToken);
    }
}
```

---

## Часть IV. AvtoBus.Transport.InMemory

### Файл: InMemoryTransport.cs

```csharp
using System.Collections.Concurrent;
using System.Threading.Channels;
using AvtoBus.Abstractions;

namespace AvtoBus.Transport.InMemory;

/// <summary>
/// In-memory transport для тестов и local queues.
/// Каждый endpoint — bounded channel; redelivery при nack.
/// </summary>
public sealed class InMemoryTransport : IAvtoTransport
{
    public string Name => "inmemory";
    public AvtoTransportCapabilities Capabilities =>
        AvtoTransportCapabilities.Queues | AvtoTransportCapabilities.Topics |
        AvtoTransportCapabilities.DelayedDelivery;

    private readonly ConcurrentDictionary<string, Channel<AvtoEnvelope>> _queues = new();
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>Все отправленные envelope — для test harness.</summary>
    public ConcurrentQueue<AvtoOutgoing> SentLog { get; } = new();

    private Channel<AvtoEnvelope> GetQueue(string endpoint)
        => _queues.GetOrAdd(endpoint, _ => Channel.CreateBounded<AvtoEnvelope>(
            new BoundedChannelOptions(10_000) { FullMode = BoundedChannelFullMode.Wait }));

    public async ValueTask SendAsync(AvtoOutgoing outgoing, CancellationToken ct)
    {
        SentLog.Enqueue(outgoing);

        var envelope = outgoing.Envelope;
        if (envelope.NotBefore is { } notBefore && notBefore > DateTimeOffset.UtcNow)
        {
            var delay = notBefore - DateTimeOffset.UtcNow;
            _ = DeliverDelayedAsync(outgoing.Destination, envelope, delay);
            return;
        }

        await GetQueue(outgoing.Destination).Writer.WriteAsync(envelope, ct);
    }

    private async Task DeliverDelayedAsync(string destination, AvtoEnvelope envelope, TimeSpan delay)
    {
        try
        {
            await Task.Delay(delay, _shutdown.Token);
            await GetQueue(destination).Writer.WriteAsync(envelope, _shutdown.Token);
        }
        catch (OperationCanceledException) { }
    }

    public ValueTask<IAsyncDisposable> SubscribeAsync(
        string endpoint, AvtoDeliveryHandler handler, CancellationToken ct)
    {
        var queue = GetQueue(endpoint);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _shutdown.Token);

        var loop = Task.Run(async () =>
        {
            await foreach (var envelope in queue.Reader.ReadAllAsync(cts.Token))
            {
                var acked = false;
                try { acked = await handler(envelope, cts.Token); }
                catch { /* nack */ }

                if (!acked)
                {
                    envelope.DeliveryAttempt++;
                    await queue.Writer.WriteAsync(envelope, cts.Token); // redeliver
                }
            }
        }, cts.Token);

        return ValueTask.FromResult<IAsyncDisposable>(new Subscription(cts, loop));
    }

    private sealed class Subscription(CancellationTokenSource cts, Task loop) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await cts.CancelAsync();
            try { await loop; } catch (OperationCanceledException) { }
            cts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync();
        _shutdown.Dispose();
    }
}
```

### Файл: InMemoryDurability.cs

```csharp
using System.Collections.Concurrent;
using AvtoBus.Abstractions;

namespace AvtoBus.Transport.InMemory;

public sealed class InMemoryOutboxStore(TimeProvider clock) : IOutboxStore
{
    private readonly ConcurrentDictionary<Guid, OutboxRecord> _records = new();
    private readonly object _claimLock = new();

    public ValueTask AddAsync(IReadOnlyList<OutboxRecord> records, CancellationToken ct)
    {
        foreach (var r in records) _records[r.Id] = r;
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(
        int batchSize, string claimedBy, TimeSpan lockDuration, CancellationToken ct)
    {
        lock (_claimLock)
        {
            var now = clock.GetUtcNow();
            var claimed = _records.Values
                .Where(r => r.State == OutboxState.Pending && r.NextAttemptAt <= now)
                .OrderBy(r => r.CreatedAt)
                .Take(batchSize)
                .ToList();

            foreach (var r in claimed) r.State = OutboxState.Dispatching;
            return ValueTask.FromResult<IReadOnlyList<OutboxRecord>>(claimed);
        }
    }

    public ValueTask MarkDispatchedAsync(IReadOnlyList<Guid> ids, CancellationToken ct)
    {
        foreach (var id in ids)
            if (_records.TryGetValue(id, out var r))
            {
                r.State = OutboxState.Dispatched;
                r.DispatchedAt = clock.GetUtcNow();
            }
        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAttemptAsync(
        Guid id, string error, DateTimeOffset nextAttempt, CancellationToken ct)
    {
        if (_records.TryGetValue(id, out var r))
        {
            r.State = OutboxState.Pending;
            r.AttemptCount++;
            r.LastError = error;
            r.NextAttemptAt = nextAttempt;
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask MoveToDeadLetterAsync(Guid id, string reason, CancellationToken ct)
    {
        if (_records.TryGetValue(id, out var r))
        {
            r.State = OutboxState.Failed;
            r.LastError = reason;
        }
        return ValueTask.CompletedTask;
    }

    public int PendingCount => _records.Values.Count(r => r.State == OutboxState.Pending);
}

public sealed class InMemoryInboxStore : IInboxStore
{
    private readonly ConcurrentDictionary<(Guid, string), byte> _consumed = new();

    public ValueTask<bool> IsDuplicateAsync(Guid messageId, string consumerId, CancellationToken ct)
        => ValueTask.FromResult(_consumed.ContainsKey((messageId, consumerId)));

    public ValueTask MarkConsumedAsync(
        Guid messageId, string consumerId, string messageType, CancellationToken ct)
    {
        _consumed[(messageId, consumerId)] = 1;
        return ValueTask.CompletedTask;
    }
}

public sealed class InMemorySagaStore : ISagaStore
{
    private readonly ConcurrentDictionary<string, SagaRecord> _sagas = new();
    private readonly object _saveLock = new();

    public ValueTask<SagaRecord?> LoadAsync(string sagaType, string correlationId, CancellationToken ct)
    {
        var id = $"{sagaType}:{correlationId}";
        _sagas.TryGetValue(id, out var record);
        return ValueTask.FromResult(record?.Status == "Active" ? record : null);
    }

    public ValueTask SaveAsync(SagaRecord record, CancellationToken ct)
    {
        lock (_saveLock)
        {
            if (_sagas.TryGetValue(record.Id, out var existing)
                && existing.Version != record.Version)
            {
                throw new SagaConcurrencyException(record.Id);
            }
            record.Version++;
            record.UpdatedAt = DateTimeOffset.UtcNow;
            _sagas[record.Id] = record;
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask CompleteAsync(string sagaId, CancellationToken ct)
    {
        if (_sagas.TryGetValue(sagaId, out var r)) r.Status = "Completed";
        return ValueTask.CompletedTask;
    }
}

public sealed class InMemoryDeadLetterStore : IDeadLetterStore
{
    private readonly ConcurrentDictionary<Guid, DeadLetterRecord> _records = new();

    public ValueTask AddAsync(DeadLetterRecord record, CancellationToken ct)
    {
        _records[record.Id] = record;
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<DeadLetterRecord>> ListAsync(int limit, CancellationToken ct)
        => ValueTask.FromResult<IReadOnlyList<DeadLetterRecord>>(
            _records.Values.OrderByDescending(r => r.FailedAt).Take(limit).ToList());

    public ValueTask<DeadLetterRecord?> GetAsync(Guid id, CancellationToken ct)
        => ValueTask.FromResult(_records.GetValueOrDefault(id));

    public ValueTask RemoveAsync(Guid id, CancellationToken ct)
    {
        _records.TryRemove(id, out _);
        return ValueTask.CompletedTask;
    }
}
```

---

## Часть V. AvtoBus.Durability.EFCore

> **Примечание (2026-08-27):** Актуальная EF-модель — 11 таблиц: `envelopes`, `outbox_messages`, `inbox_messages`, `sagas`, `dead_letters`, `scheduled_messages`, `event_streams`, `projection_checkpoints`, `workflow_instances`/`workflow_history`/`workflow_timers`, `schema_registry`, `store_version` — см. `src/AvtoBus.Durability.EFCore/AvtoBusModelExtensions.cs`. Ниже показана MVP-версия (4 таблицы), полная версия в коде.

### Файл: Entities.cs

```csharp
namespace AvtoBus.Durability.EFCore;

public sealed class OutboxEntity
{
    public Guid Id { get; set; }
    public string Destination { get; set; } = default!;
    public string Transport { get; set; } = default!;
    public string MessageType { get; set; } = default!;
    public string SchemaName { get; set; } = default!;
    public int SchemaVersion { get; set; }
    public Guid MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? TenantId { get; set; }
    public string? PartitionKey { get; set; }
    public string HeadersJson { get; set; } = "{}";
    public byte[] Body { get; set; } = default!;
    public string ContentType { get; set; } = "application/json";
    public string State { get; set; } = "Pending";
    public int AttemptCount { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset NextAttemptAt { get; set; }
    public DateTimeOffset? NotBefore { get; set; }
    public DateTimeOffset? DispatchedAt { get; set; }
    public string? LastError { get; set; }
    public string? LockedBy { get; set; }
    public DateTimeOffset? LockedUntil { get; set; }
}

public sealed class InboxEntity
{
    public Guid MessageId { get; set; }
    public string ConsumerId { get; set; } = default!;
    public string MessageType { get; set; } = default!;
    public DateTimeOffset ConsumedAt { get; set; }
}

public sealed class SagaEntity
{
    public string Id { get; set; } = default!;
    public string SagaType { get; set; } = default!;
    public string CorrelationId { get; set; } = default!;
    public byte[] State { get; set; } = default!;
    public long Version { get; set; }
    public string Status { get; set; } = "Active";
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

public sealed class DeadLetterEntity
{
    public Guid Id { get; set; }
    public Guid MessageId { get; set; }
    public string MessageType { get; set; } = default!;
    public string SchemaName { get; set; } = default!;
    public int SchemaVersion { get; set; }
    public byte[] Body { get; set; } = default!;
    public string Reason { get; set; } = default!;
    public string Endpoint { get; set; } = default!;
    public string? ExceptionType { get; set; }
    public int AttemptCount { get; set; }
    public DateTimeOffset FailedAt { get; set; }
}
```

### Файл: AvtoBusModelExtensions.cs

```csharp
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Durability.EFCore;

public static class AvtoBusModelExtensions
{
    /// <summary>Вызывается из OnModelCreating DbContext приложения.</summary>
    public static ModelBuilder AddAvtoBusEntities(this ModelBuilder builder, string schema = "avtobus")
    {
        builder.Entity<OutboxEntity>(e =>
        {
            e.ToTable("outbox_messages", schema);
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.State, x.NextAttemptAt })
                .HasFilter("\"State\" = 'Pending'");
            e.Property(x => x.State).HasMaxLength(16);
        });

        builder.Entity<InboxEntity>(e =>
        {
            e.ToTable("inbox_messages", schema);
            e.HasKey(x => new { x.MessageId, x.ConsumerId });
        });

        builder.Entity<SagaEntity>(e =>
        {
            e.ToTable("sagas", schema);
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.SagaType, x.CorrelationId });
            e.Property(x => x.Version).IsConcurrencyToken();
        });

        builder.Entity<DeadLetterEntity>(e =>
        {
            e.ToTable("dead_letters", schema);
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.FailedAt);
        });

        return builder;
    }
}
```

### Файл: EfCoreOutboxStore.cs

```csharp
using System.Text.Json;
using AvtoBus.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Durability.EFCore;

/// <summary>
/// EF Core outbox. AddAsync участвует в текущей транзакции DbContext приложения —
/// это и есть transactional outbox.
/// </summary>
public sealed class EfCoreOutboxStore<TDbContext>(
    TDbContext db,
    TimeProvider clock) : IOutboxStore
    where TDbContext : DbContext
{
    public ValueTask AddAsync(IReadOnlyList<OutboxRecord> records, CancellationToken ct)
    {
        foreach (var r in records)
        {
            db.Set<OutboxEntity>().Add(new OutboxEntity
            {
                Id = r.Id,
                Destination = r.Destination,
                Transport = r.Transport,
                MessageId = r.Envelope.MessageId,
                MessageType = r.Envelope.MessageType,
                SchemaName = r.Envelope.SchemaName,
                SchemaVersion = r.Envelope.SchemaVersion,
                CorrelationId = r.Envelope.CorrelationId,
                CausationId = r.Envelope.CausationId,
                TenantId = r.Envelope.TenantId,
                PartitionKey = r.Envelope.PartitionKey,
                HeadersJson = JsonSerializer.Serialize(r.Envelope.Headers),
                Body = r.Envelope.Body,
                ContentType = r.Envelope.ContentType,
                CreatedAt = r.CreatedAt,
                NextAttemptAt = r.NextAttemptAt,
                NotBefore = r.Envelope.NotBefore,
            });
        }
        // SaveChanges вызывает UnitOfWork приложения (или pipeline).
        return ValueTask.CompletedTask;
    }

    public async ValueTask<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(
        int batchSize, string claimedBy, TimeSpan lockDuration, CancellationToken ct)
    {
        var now = clock.GetUtcNow();
        var until = now + lockDuration;

        // PostgreSQL: FOR UPDATE SKIP LOCKED через raw SQL.
        var entities = await db.Set<OutboxEntity>()
            .FromSqlInterpolated($"""
                UPDATE avtobus.outbox_messages
                SET "State" = 'Dispatching', "LockedBy" = {claimedBy}, "LockedUntil" = {until}
                WHERE "Id" IN (
                    SELECT "Id" FROM avtobus.outbox_messages
                    WHERE "State" = 'Pending' AND "NextAttemptAt" <= {now}
                    ORDER BY "CreatedAt"
                    LIMIT {batchSize}
                    FOR UPDATE SKIP LOCKED)
                RETURNING *
                """)
            .AsNoTracking()
            .ToListAsync(ct);

        return entities.Select(ToRecord).ToList();
    }

    public async ValueTask MarkDispatchedAsync(IReadOnlyList<Guid> ids, CancellationToken ct)
    {
        var now = clock.GetUtcNow();
        await db.Set<OutboxEntity>()
            .Where(e => ids.Contains(e.Id))
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.State, "Dispatched")
                .SetProperty(e => e.DispatchedAt, now)
                .SetProperty(e => e.LockedBy, (string?)null), ct);
    }

    public async ValueTask MarkFailedAttemptAsync(
        Guid id, string error, DateTimeOffset nextAttempt, CancellationToken ct)
    {
        await db.Set<OutboxEntity>()
            .Where(e => e.Id == id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.State, "Pending")
                .SetProperty(e => e.AttemptCount, e => e.AttemptCount + 1)
                .SetProperty(e => e.LastError, error)
                .SetProperty(e => e.NextAttemptAt, nextAttempt)
                .SetProperty(e => e.LockedBy, (string?)null), ct);
    }

    public async ValueTask MoveToDeadLetterAsync(Guid id, string reason, CancellationToken ct)
    {
        await db.Set<OutboxEntity>()
            .Where(e => e.Id == id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.State, "Failed")
                .SetProperty(e => e.LastError, reason), ct);
    }

    private static OutboxRecord ToRecord(OutboxEntity e) => new()
    {
        Id = e.Id,
        Destination = e.Destination,
        Transport = e.Transport,
        AttemptCount = e.AttemptCount,
        NextAttemptAt = e.NextAttemptAt,
        Envelope = new AvtoEnvelope
        {
            MessageId = e.MessageId,
            MessageType = e.MessageType,
            SchemaName = e.SchemaName,
            SchemaVersion = e.SchemaVersion,
            CorrelationId = e.CorrelationId,
            CausationId = e.CausationId,
            TenantId = e.TenantId,
            PartitionKey = e.PartitionKey,
            Headers = JsonSerializer.Deserialize<Dictionary<string, string>>(e.HeadersJson) ?? [],
            Body = e.Body,
            ContentType = e.ContentType,
            CreatedAt = e.CreatedAt,
            NotBefore = e.NotBefore,
        },
    };
}
```

### Файл: EfCoreInboxStore.cs и EfCoreSagaStore.cs

```csharp
using AvtoBus.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Durability.EFCore;

public sealed class EfCoreInboxStore<TDbContext>(TDbContext db, TimeProvider clock) : IInboxStore
    where TDbContext : DbContext
{
    public async ValueTask<bool> IsDuplicateAsync(Guid messageId, string consumerId, CancellationToken ct)
        => await db.Set<InboxEntity>()
            .AnyAsync(e => e.MessageId == messageId && e.ConsumerId == consumerId, ct);

    public ValueTask MarkConsumedAsync(
        Guid messageId, string consumerId, string messageType, CancellationToken ct)
    {
        db.Set<InboxEntity>().Add(new InboxEntity
        {
            MessageId = messageId,
            ConsumerId = consumerId,
            MessageType = messageType,
            ConsumedAt = clock.GetUtcNow(),
        });
        return ValueTask.CompletedTask; // SaveChanges — вместе с бизнес-транзакцией
    }
}

public sealed class EfCoreSagaStore<TDbContext>(TDbContext db, TimeProvider clock) : ISagaStore
    where TDbContext : DbContext
{
    public async ValueTask<SagaRecord?> LoadAsync(
        string sagaType, string correlationId, CancellationToken ct)
    {
        var entity = await db.Set<SagaEntity>()
            .AsNoTracking()
            .FirstOrDefaultAsync(e =>
                e.SagaType == sagaType &&
                e.CorrelationId == correlationId &&
                e.Status == "Active", ct);

        return entity is null ? null : new SagaRecord
        {
            Id = entity.Id,
            SagaType = entity.SagaType,
            CorrelationId = entity.CorrelationId,
            State = entity.State,
            Version = entity.Version,
            Status = entity.Status,
        };
    }

    public async ValueTask SaveAsync(SagaRecord record, CancellationToken ct)
    {
        var now = clock.GetUtcNow();
        if (record.Version == 0)
        {
            db.Set<SagaEntity>().Add(new SagaEntity
            {
                Id = record.Id,
                SagaType = record.SagaType,
                CorrelationId = record.CorrelationId,
                State = record.State,
                Version = 1,
                Status = record.Status,
                CreatedAt = now,
                UpdatedAt = now,
            });
        }
        else
        {
            var updated = await db.Set<SagaEntity>()
                .Where(e => e.Id == record.Id && e.Version == record.Version)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(e => e.State, record.State)
                    .SetProperty(e => e.Version, record.Version + 1)
                    .SetProperty(e => e.Status, record.Status)
                    .SetProperty(e => e.UpdatedAt, now), ct);

            if (updated == 0)
                throw new SagaConcurrencyException(record.Id);
        }
        record.Version++;
    }

    public async ValueTask CompleteAsync(string sagaId, CancellationToken ct)
    {
        await db.Set<SagaEntity>()
            .Where(e => e.Id == sagaId)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Status, "Completed"), ct);
    }
}
```

---

## Часть VI. AvtoBus.Transport.RabbitMQ

### Файл: RabbitMqTransport.cs

```csharp
using System.Text;
using AvtoBus.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AvtoBus.Transport.RabbitMQ;

public sealed class RabbitMqOptions
{
    public required string ConnectionString { get; init; }
    public bool AutoProvision { get; init; } = true;
    public bool UseQuorumQueues { get; init; } = true;
    public ushort PrefetchCount { get; init; } = 64;
}

public sealed class RabbitMqTransport(RabbitMqOptions options) : IAvtoTransport
{
    public string Name => "rabbitmq";
    public AvtoTransportCapabilities Capabilities =>
        AvtoTransportCapabilities.Queues | AvtoTransportCapabilities.Topics |
        AvtoTransportCapabilities.NativeDeadLetter;

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    private async ValueTask<IConnection> GetConnectionAsync(CancellationToken ct)
    {
        if (_connection is { IsOpen: true }) return _connection;
        await _connectLock.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;
            var factory = new ConnectionFactory
            {
                Uri = new Uri(options.ConnectionString),
                AutomaticRecoveryEnabled = true,
            };
            _connection = await factory.CreateConnectionAsync(ct);
            return _connection;
        }
        finally { _connectLock.Release(); }
    }

    private async ValueTask<IChannel> GetPublishChannelAsync(CancellationToken ct)
    {
        if (_publishChannel is { IsOpen: true }) return _publishChannel;
        var conn = await GetConnectionAsync(ct);
        _publishChannel = await conn.CreateChannelAsync(cancellationToken: ct);
        return _publishChannel;
    }

    public async ValueTask SendAsync(AvtoOutgoing outgoing, CancellationToken ct)
    {
        var channel = await GetPublishChannelAsync(ct);
        var envelope = outgoing.Envelope;

        if (options.AutoProvision)
            await DeclareQueueAsync(channel, outgoing.Destination, ct);

        var props = new BasicProperties
        {
            MessageId = envelope.MessageId.ToString("N"),
            ContentType = envelope.ContentType,
            Persistent = true,
            Type = envelope.SchemaName,
            CorrelationId = envelope.CorrelationId,
            Headers = new Dictionary<string, object?>
            {
                ["avto-schema-name"] = envelope.SchemaName,
                ["avto-schema-version"] = envelope.SchemaVersion,
                ["avto-message-type"] = envelope.MessageType,
                ["avto-causation-id"] = envelope.CausationId,
                ["avto-tenant-id"] = envelope.TenantId,
                ["traceparent"] = envelope.TraceParent,
            },
        };

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: outgoing.Destination,
            mandatory: false,
            basicProperties: props,
            body: envelope.Body,
            cancellationToken: ct);
    }

    public async ValueTask<IAsyncDisposable> SubscribeAsync(
        string endpoint, AvtoDeliveryHandler handler, CancellationToken ct)
    {
        var conn = await GetConnectionAsync(ct);
        var channel = await conn.CreateChannelAsync(cancellationToken: ct);
        await channel.BasicQosAsync(0, options.PrefetchCount, false, ct);

        if (options.AutoProvision)
            await DeclareQueueAsync(channel, endpoint, ct);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var envelope = ToEnvelope(ea);
            bool acked;
            try { acked = await handler(envelope, CancellationToken.None); }
            catch { acked = false; }

            if (acked)
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            else
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
        };

        var tag = await channel.BasicConsumeAsync(endpoint, autoAck: false, consumer, ct);
        return new Subscription(channel, tag);
    }

    private async Task DeclareQueueAsync(IChannel channel, string queue, CancellationToken ct)
    {
        var args = options.UseQuorumQueues
            ? new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }
            : null;
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false,
            autoDelete: false, arguments: args, cancellationToken: ct);
    }

    private static AvtoEnvelope ToEnvelope(BasicDeliverEventArgs ea)
    {
        var headers = ea.BasicProperties.Headers ?? new Dictionary<string, object?>();
        string? Header(string key)
            => headers.TryGetValue(key, out var v)
                ? v switch { byte[] b => Encoding.UTF8.GetString(b), string s => s, _ => v?.ToString() }
                : null;

        return new AvtoEnvelope
        {
            MessageId = Guid.TryParse(ea.BasicProperties.MessageId, out var id) ? id : Guid.NewGuid(),
            MessageType = Header("avto-message-type") ?? ea.BasicProperties.Type ?? "unknown",
            SchemaName = Header("avto-schema-name") ?? ea.BasicProperties.Type ?? "unknown",
            SchemaVersion = int.TryParse(Header("avto-schema-version"), out var v) ? v : 1,
            CorrelationId = ea.BasicProperties.CorrelationId,
            CausationId = Header("avto-causation-id"),
            TenantId = Header("avto-tenant-id"),
            TraceParent = Header("traceparent"),
            ContentType = ea.BasicProperties.ContentType ?? "application/json",
            CreatedAt = DateTimeOffset.UtcNow,
            Body = ea.Body.ToArray(),
        };
    }

    private sealed class Subscription(IChannel channel, string consumerTag) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { await channel.BasicCancelAsync(consumerTag); } catch { }
            await channel.DisposeAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_publishChannel is not null) await _publishChannel.DisposeAsync();
        if (_connection is not null) await _connection.DisposeAsync();
        _connectLock.Dispose();
    }
}
```

---

## Часть VII. AvtoBus.Hosting

### Файл: AvtoBusBuilder.cs

```csharp
using System.Reflection;
using AvtoBus.Abstractions;
using AvtoBus.Core.Dispatch;
using AvtoBus.Core.Routing;

namespace AvtoBus.Hosting;

public sealed class AvtoBusBuilder
{
    internal string ApplicationName { get; private set; } = "avtobus-app";
    internal List<Assembly> HandlerAssemblies { get; } = [];
    internal List<Type> SagaTypes { get; } = [];
    internal RouteTable Routes { get; } = new();
    internal List<ReceiveEndpoint> ReceiveEndpoints { get; } = [];
    internal Dictionary<string, Func<IServiceProvider, IAvtoTransport>> TransportFactories { get; } = new();
    internal OutboxDispatcherOptions DispatcherOptions { get; } = new();
    internal bool UseInMemoryDefaults { get; private set; }

    public AvtoBusBuilder WithApplicationName(string name)
    {
        ApplicationName = name;
        return this;
    }

    public AvtoBusBuilder AddHandlersFromAssemblyContaining<T>()
    {
        HandlerAssemblies.Add(typeof(T).Assembly);
        return this;
    }

    public AvtoBusBuilder AddSaga<TSaga>() where TSaga : AvtoSaga, new()
    {
        SagaTypes.Add(typeof(TSaga));
        return this;
    }

    public AvtoBusBuilder UseInMemory()
    {
        UseInMemoryDefaults = true;
        return this;
    }

    public AvtoBusBuilder AddTransport(string name, Func<IServiceProvider, IAvtoTransport> factory)
    {
        TransportFactories[name] = factory;
        return this;
    }

    public AvtoBusBuilder RouteCommand<T>(string transport, string destination)
    {
        Routes.Add(new RouteEntry(typeof(T), RouteKind.Command, transport, destination));
        return this;
    }

    public AvtoBusBuilder RouteEvent<T>(string transport, string destination)
    {
        Routes.Add(new RouteEntry(typeof(T), RouteKind.Event, transport, destination));
        return this;
    }

    public AvtoBusBuilder ListenOn(string transport, string endpoint)
    {
        ReceiveEndpoints.Add(new ReceiveEndpoint(transport, endpoint));
        return this;
    }

    public AvtoBusBuilder ConfigureDispatcher(Action<OutboxDispatcherOptions> configure)
    {
        configure(DispatcherOptions);
        return this;
    }
}
```

### Файл: ServiceCollectionExtensions.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Core.Bus;
using AvtoBus.Core.Dispatch;
using AvtoBus.Core.Handlers;
using AvtoBus.Core.Pipeline;
using AvtoBus.Core.Routing;
using AvtoBus.Core.Sagas;
using AvtoBus.Core.Serialization;
using AvtoBus.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAvtoBus(
        this IServiceCollection services, Action<AvtoBusBuilder> configure)
    {
        var builder = new AvtoBusBuilder();
        configure(builder);

        // Core singletons
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<IAvtoSerializer, SystemTextJsonSerializer>();
        services.AddSingleton<IAvtoMessageTypeRegistry, MessageTypeRegistry>();
        services.AddSingleton(builder.Routes);
        services.AddSingleton<EnvelopeFactory>();
        services.AddSingleton<RetryPolicy>();
        services.AddSingleton(builder.DispatcherOptions);

        // Handlers: discovery (reflection MVP; source generation заменяет это)
        services.AddSingleton(sp =>
        {
            var registry = new HandlerRegistry();
            foreach (var assembly in builder.HandlerAssemblies)
                foreach (var invoker in ReflectionHandlerInvoker.Discover(assembly))
                    registry.Add(invoker);
            return registry;
        });

        // Sagas
        services.AddSingleton<IReadOnlyList<IAvtoSagaDescriptor>>(_ =>
            builder.SagaTypes.Select(t => (IAvtoSagaDescriptor)new ReflectionSagaDescriptor(t)).ToList());
        services.AddSingleton<SagaRuntime>();

        // Pipeline
        services.AddSingleton<HandlerPipeline>();
        services.AddScoped<EffectMaterializer>();

        // In-memory defaults (переопределяются durability-пакетами)
        if (builder.UseInMemoryDefaults)
        {
            services.AddSingleton<InMemoryTransport>();
            services.AddSingleton<InMemoryOutboxStore>();
            services.TryAddSingleton<IOutboxStore>(sp => sp.GetRequiredService<InMemoryOutboxStore>());
            services.TryAddSingleton<IInboxStore, InMemoryInboxStore>();
            services.TryAddSingleton<ISagaStore, InMemorySagaStore>();
            services.TryAddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();
            builder.TransportFactories.TryAdd("inmemory",
                sp => sp.GetRequiredService<InMemoryTransport>());
        }

        // Transports map
        services.AddSingleton<IReadOnlyDictionary<string, IAvtoTransport>>(sp =>
            builder.TransportFactories.ToDictionary(kv => kv.Key, kv => kv.Value(sp)));

        // Bus
        services.AddSingleton<IAvtoBus, AvtoBusClient>();

        // Hosted services
        services.AddSingleton<IReadOnlyList<ReceiveEndpoint>>(builder.ReceiveEndpoints);
        services.AddHostedService<OutboxDispatcherService>();
        services.AddHostedService<TransportReceiverService>();

        return services;
    }
}
```

---

## Часть VIII. AvtoBus.Testing

### Файл: AvtoBusTestHost.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Core.Dispatch;
using AvtoBus.Hosting;
using AvtoBus.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Testing;

public sealed class AvtoBusTestHost : IAsyncDisposable
{
    private readonly IHost _host;

    public IAvtoBus Bus { get; }
    public IServiceProvider Services => _host.Services;
    public InMemoryTransport Transport { get; }
    public InMemoryOutboxStore Outbox { get; }
    public IDeadLetterStore DeadLetters { get; }

    private AvtoBusTestHost(IHost host)
    {
        _host = host;
        Bus = host.Services.GetRequiredService<IAvtoBus>();
        Transport = host.Services.GetRequiredService<InMemoryTransport>();
        Outbox = host.Services.GetRequiredService<InMemoryOutboxStore>();
        DeadLetters = host.Services.GetRequiredService<IDeadLetterStore>();
    }

    public static async Task<AvtoBusTestHost> CreateAsync(
        Action<AvtoBusBuilder> configureBus,
        Action<IServiceCollection>? configureServices = null)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAvtoBus(bus =>
        {
            bus.UseInMemory();
            bus.ConfigureDispatcher(o => o.PollingInterval = TimeSpan.FromMilliseconds(20));
            configureBus(bus);
        });
        configureServices?.Invoke(hostBuilder.Services);

        var host = hostBuilder.Build();
        await host.StartAsync();
        return new AvtoBusTestHost(host);
    }

    /// <summary>Подождать, пока условие станет истинным (eventual assertions).</summary>
    public async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTimeOffset.UtcNow > deadline)
                throw new TimeoutException("Condition not met within timeout.");
            await Task.Delay(25);
        }
    }

    /// <summary>Было ли отправлено сообщение типа T в транспорт.</summary>
    public bool WasSent<T>(Func<T, bool>? predicate = null)
        => Transport.SentLog.Any(o =>
            o.Envelope.Message is T typed && (predicate?.Invoke(typed) ?? true));

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync(TimeSpan.FromSeconds(5));
        _host.Dispose();
    }
}
```

---

## Часть IX. Пример приложения

### Контракты

```csharp
using AvtoBus.Abstractions;

namespace OrderShop.Contracts;

public sealed record OrderLine(string Sku, int Quantity);

public sealed record SubmitOrder(Guid OrderId, Guid CustomerId, IReadOnlyList<OrderLine> Lines)
    : ICommand<OrderAccepted>, IPartitionedMessage
{
    public static string SchemaName => "ordershop.orders.submit-order";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record OrderAccepted(Guid OrderId, DateTimeOffset AcceptedAt);

public sealed record OrderSubmitted(Guid OrderId, Guid CustomerId, DateTimeOffset SubmittedAt)
    : IEvent, IPartitionedMessage
{
    public static string SchemaName => "ordershop.orders.order-submitted";
    public static int SchemaVersion => 1;
    public string PartitionKey => OrderId.ToString("N");
}

public sealed record CapturePayment(Guid OrderId) : ICommand
{
    public static string SchemaName => "ordershop.payments.capture";
    public static int SchemaVersion => 1;
}

public sealed record PaymentCaptured(Guid OrderId) : IEvent
{
    public static string SchemaName => "ordershop.payments.captured";
    public static int SchemaVersion => 1;
}

public sealed record ReserveInventory(Guid OrderId) : ICommand
{
    public static string SchemaName => "ordershop.inventory.reserve";
    public static int SchemaVersion => 1;
}

public sealed record InventoryReserved(Guid OrderId) : IEvent
{
    public static string SchemaName => "ordershop.inventory.reserved";
    public static int SchemaVersion => 1;
}

public sealed record OrderReadyToShip(Guid OrderId) : IEvent
{
    public static string SchemaName => "ordershop.orders.ready-to-ship";
    public static int SchemaVersion => 1;
}

public sealed record FulfillmentTimedOut(Guid OrderId) : ICommand
{
    public static string SchemaName => "ordershop.orders.fulfillment-timed-out";
    public static int SchemaVersion => 1;
}
```

### Handler

```csharp
using AvtoBus.Abstractions;
using OrderShop.Contracts;

namespace OrderShop.Api.Handlers;

public static class SubmitOrderHandler
{
    public static ValidationResult Validate(SubmitOrder command)
        => command.Lines.Count == 0
            ? ValidationResult.Invalid("Order must have at least one line.")
            : ValidationResult.Valid;

    public static (OrderAccepted, OrderSubmitted) Handle(SubmitOrder command, TimeProvider clock)
    {
        var now = clock.GetUtcNow();
        // Здесь: db.Orders.Add(...) при подключенном EF Core durability.
        return (
            new OrderAccepted(command.OrderId, now),
            new OrderSubmitted(command.OrderId, command.CustomerId, now));
    }
}
```

### Saga

```csharp
using AvtoBus.Abstractions;
using OrderShop.Contracts;

namespace OrderShop.Api.Sagas;

public sealed class OrderFulfillmentSaga : AvtoSaga
{
    public Guid OrderId { get; set; }
    public bool PaymentCaptured { get; set; }
    public bool InventoryReserved { get; set; }

    public static Guid Correlate(OrderSubmitted m) => m.OrderId;
    public static Guid Correlate(PaymentCaptured m) => m.OrderId;
    public static Guid Correlate(InventoryReserved m) => m.OrderId;
    public static Guid Correlate(FulfillmentTimedOut m) => m.OrderId;

    public AvtoEffects Start(OrderSubmitted @event)
    {
        OrderId = @event.OrderId;
        return AvtoEffects.All(
            AvtoEffects.Send(new CapturePayment(OrderId)),
            AvtoEffects.Send(new ReserveInventory(OrderId)),
            AvtoEffects.Schedule(new FulfillmentTimedOut(OrderId), TimeSpan.FromMinutes(15)));
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

    public AvtoEffects Handle(FulfillmentTimedOut @event)
        => PaymentCaptured && InventoryReserved
            ? AvtoEffects.None
            : AvtoEffects.CompleteSaga();

    private AvtoEffects TryComplete()
        => PaymentCaptured && InventoryReserved
            ? AvtoEffects.All(
                AvtoEffects.Publish(new OrderReadyToShip(OrderId)),
                AvtoEffects.CompleteSaga())
            : AvtoEffects.None;
}
```

### Program.cs

```csharp
using AvtoBus.Abstractions;
using AvtoBus.Hosting;
using OrderShop.Api.Handlers;
using OrderShop.Api.Sagas;
using OrderShop.Contracts;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAvtoBus(bus => bus
    .WithApplicationName("OrderShop.Api")
    .AddHandlersFromAssemblyContaining<SubmitOrderHandler>()
    .AddSaga<OrderFulfillmentSaga>()
    .UseInMemory()   // dev; prod: .AddTransport("rabbitmq", sp => new RabbitMqTransport(...))
    .RouteCommand<SubmitOrder>("inmemory", "orders.commands")
    .RouteCommand<CapturePayment>("inmemory", "billing.commands")
    .RouteCommand<ReserveInventory>("inmemory", "inventory.commands")
    .RouteCommand<FulfillmentTimedOut>("inmemory", "orders.commands")
    .RouteEvent<OrderSubmitted>("inmemory", "orders.events")
    .RouteEvent<PaymentCaptured>("inmemory", "orders.events")
    .RouteEvent<InventoryReserved>("inmemory", "orders.events")
    .RouteEvent<OrderReadyToShip>("inmemory", "orders.events")
    .ListenOn("inmemory", "orders.commands")
    .ListenOn("inmemory", "billing.commands")
    .ListenOn("inmemory", "inventory.commands")
    .ListenOn("inmemory", "orders.events"));

var app = builder.Build();

app.MapPost("/orders", async (SubmitOrder command, IAvtoBus bus, CancellationToken ct) =>
{
    var accepted = await bus.InvokeAsync<OrderAccepted>(command, ct);
    return Results.Accepted($"/orders/{accepted.OrderId}", accepted);
});

app.Run();
```

### Тест

```csharp
using AvtoBus.Testing;
using OrderShop.Api.Handlers;
using OrderShop.Api.Sagas;
using OrderShop.Contracts;
using Xunit;

public class OrderFlowTests
{
    [Fact]
    public async Task submit_order_runs_saga_to_ready_to_ship()
    {
        await using var host = await AvtoBusTestHost.CreateAsync(bus => bus
            .AddHandlersFromAssemblyContaining<SubmitOrderHandler>()
            .AddSaga<OrderFulfillmentSaga>()
            .RouteCommand<SubmitOrder>("inmemory", "orders.commands")
            .RouteCommand<CapturePayment>("inmemory", "billing.commands")
            .RouteCommand<ReserveInventory>("inmemory", "inventory.commands")
            .RouteCommand<FulfillmentTimedOut>("inmemory", "orders.commands")
            .RouteEvent<OrderSubmitted>("inmemory", "orders.events")
            .RouteEvent<PaymentCaptured>("inmemory", "orders.events")
            .RouteEvent<InventoryReserved>("inmemory", "orders.events")
            .RouteEvent<OrderReadyToShip>("inmemory", "orders.events")
            .ListenOn("inmemory", "orders.commands")
            .ListenOn("inmemory", "billing.commands")
            .ListenOn("inmemory", "inventory.commands")
            .ListenOn("inmemory", "orders.events"));

        var command = new SubmitOrder(Guid.NewGuid(), Guid.NewGuid(),
            [new OrderLine("SKU-1", 2)]);

        var accepted = await host.Bus.InvokeAsync<OrderAccepted>(command);
        Assert.Equal(command.OrderId, accepted.OrderId);

        // Saga отправила команды payment/inventory через outbox → transport
        await host.WaitUntilAsync(() =>
            host.WasSent<CapturePayment>(c => c.OrderId == command.OrderId) &&
            host.WasSent<ReserveInventory>(c => c.OrderId == command.OrderId));

        // Симулируем ответы downstream-сервисов
        await host.Bus.PublishAsync(new PaymentCaptured(command.OrderId));
        await host.Bus.PublishAsync(new InventoryReserved(command.OrderId));

        await host.WaitUntilAsync(() =>
            host.WasSent<OrderReadyToShip>(e => e.OrderId == command.OrderId));
    }
}
```

---

## Часть X. Что дальше (за пределами этого документа)

| Компонент | Статус в этом документе | Production-путь |
| --- | --- | --- |
| Handler invokers | Reflection MVP | Source generator с тем же `IAvtoHandlerInvoker` |
| Saga descriptors | Reflection MVP | Source generator с тем же `IAvtoSagaDescriptor` |
| Serialization | Runtime System.Text.Json | `JsonSerializerContext` source generation (AOT) |
| Транзакционность EF Core | AddAsync + SaveChanges приложения | UnitOfWork middleware: inbox+outbox+бизнес в одной транзакции |
| Scheduled retry | Immediate retry only | Отдельный scheduled store + повторный enqueue |
| Kafka transport | Нет | `AvtoBus.Transport.Kafka` c consumer groups и offset commit |
| Dashboard | Нет | `AvtoBus.Dashboard`: outbox lag, DLQ, replay |
| Schema registry | Registry типов | Compatibility checks, AsyncAPI export |
| Workflow engine | Нет | `AvtoBus.Workflow`: history, timers, signals, replay |

Ключевой инвариант дизайна: reflection-компоненты и будущие source-generated компоненты реализуют одинаковые контракты (`IAvtoHandlerInvoker`, `IAvtoSagaDescriptor`), поэтому переход на генерацию не меняет ни runtime, ни пользовательский код.
