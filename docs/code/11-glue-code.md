# AvtoBus.Core — Склейка (недостающие типы)

> **Code sketch / unverified.** Этот файл фиксирует пробелы предыдущих эскизов, но не доказывает совместную компиляцию. Канонический статус: [`../FINAL.md`](../FINAL.md).

Все типы, на которые ссылается код в `code/01..10`, но которые не были определены.
Без этих контрактов предыдущие эскизы неполны; после переноса в `src/` потребуется свести дубли и проверить компиляцию.

---

## AvtoBus.Core/Serialization/ISerializer.cs

```csharp
namespace AvtoBus.Serialization;

/// <summary>
/// Сериализация тела сообщения.
/// </summary>
public interface ISerializer
{
    string ContentType { get; }
    ReadOnlyMemory<byte> Serialize(object message);
    object Deserialize(ReadOnlyMemory<byte> body, Type targetType);
    T Deserialize<T>(ReadOnlyMemory<byte> body) where T : class
        => (T)Deserialize(body, typeof(T));
}
```

---

## AvtoBus.Core/Serialization/DefaultJsonSerializer.cs

```csharp
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AvtoBus.Serialization;

/// <summary>
/// Сериализатор на System.Text.Json.
/// Использует source-generated JsonSerializerContext, если он зарегистрирован (AOT).
/// </summary>
public sealed class DefaultJsonSerializer : ISerializer
{
    public string ContentType => "application/json";

    private readonly JsonSerializerOptions _options;

    public DefaultJsonSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? CreateDefaultOptions();
    }

    public static JsonSerializerOptions CreateDefaultOptions() => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        WriteIndented = false,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };

    public ReadOnlyMemory<byte> Serialize(object message)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
        {
            Indented = false,
            SkipValidation = true,
        });

        JsonSerializer.Serialize(writer, message, message.GetType(), _options);
        writer.Flush();

        return buffer.WrittenMemory;
    }

    public object Deserialize(ReadOnlyMemory<byte> body, Type targetType)
    {
        var reader = new Utf8JsonReader(body.Span);
        var result = JsonSerializer.Deserialize(ref reader, targetType, _options);

        return result ?? throw new SerializationException(
            $"Deserialization of {targetType.Name} returned null");
    }
}

public sealed class SerializationException : Exception
{
    public SerializationException(string message) : base(message) { }
    public SerializationException(string message, Exception inner) : base(message, inner) { }
}
```

---

## AvtoBus.Core/Serialization/CompositeSerializer.cs

```csharp
namespace AvtoBus.Serialization;

/// <summary>
/// Выбирает сериализатор по ContentType входящего сообщения.
/// Позволяет одновременно принимать JSON и MessagePack (миграция форматов).
/// </summary>
public sealed class CompositeSerializer : ISerializer
{
    private readonly Dictionary<string, ISerializer> _byContentType;
    private readonly ISerializer _default;

    public CompositeSerializer(IEnumerable<ISerializer> serializers, string defaultContentType = "application/json")
    {
        _byContentType = serializers.ToDictionary(s => s.ContentType, StringComparer.OrdinalIgnoreCase);
        _default = _byContentType.GetValueOrDefault(defaultContentType)
            ?? _byContentType.Values.First();
    }

    public string ContentType => _default.ContentType;

    public ReadOnlyMemory<byte> Serialize(object message) => _default.Serialize(message);

    public object Deserialize(ReadOnlyMemory<byte> body, Type targetType)
        => _default.Deserialize(body, targetType);

    /// <summary>
    /// Десериализация с явным указанием формата (из Envelope.ContentType).
    /// </summary>
    public object Deserialize(ReadOnlyMemory<byte> body, Type targetType, string contentType)
    {
        var serializer = _byContentType.GetValueOrDefault(contentType) ?? _default;
        return serializer.Deserialize(body, targetType);
    }
}
```

---

## AvtoBus.Core/IConsumer.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Интерфейсный стиль обработчика (Rebus/MassTransit-подобный).
/// </summary>
public interface IConsumer<in TMessage> where TMessage : class
{
    Task Consume(ConsumeContext<TMessage> context);
}

/// <summary>
/// Упрощённый обработчик без контекста.
/// </summary>
public interface IHandleMessages<in TMessage> where TMessage : class
{
    Task Handle(TMessage message);
}

/// <summary>
/// Маркер команды: ровно один получатель.
/// </summary>
public interface ICommand;

/// <summary>
/// Маркер события: 0..N получателей.
/// </summary>
public interface IEvent;

/// <summary>
/// Маркер запроса (request/response).
/// </summary>
public interface IRequest<TReply> where TReply : class;

/// <summary>
/// Обработчик неуспешных сообщений (second-level retry).
/// </summary>
public interface IHandleFailed<in TMessage> where TMessage : class
{
    Task Handle(IFailed<TMessage> failed);
}

public interface IFailed<out TMessage> where TMessage : class
{
    TMessage Message { get; }
    Envelope Envelope { get; }
    string ErrorDescription { get; }
    IReadOnlyList<ExceptionInfo> Exceptions { get; }
}

public sealed record ExceptionInfo(
    string Type,
    string Message,
    string? StackTrace,
    DateTimeOffset OccurredAt,
    int Attempt);

internal sealed class FailedMessage<TMessage> : IFailed<TMessage> where TMessage : class
{
    public required TMessage Message { get; init; }
    public required Envelope Envelope { get; init; }
    public required string ErrorDescription { get; init; }
    public required IReadOnlyList<ExceptionInfo> Exceptions { get; init; }
}
```

---

## AvtoBus.Core/Sagas/ISagaInterfaces.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Сага начинается этим сообщением.
/// </summary>
public interface IStartedBy<in TMessage> where TMessage : class
{
    Task Handle(TMessage message);
}

/// <summary>
/// Сага обрабатывает это сообщение (инстанс должен существовать).
/// </summary>
public interface IHandle<in TMessage> where TMessage : class
{
    Task Handle(TMessage message);
}

/// <summary>
/// Фабрика контекста саги.
/// </summary>
public interface ISagaContextFactory
{
    ISagaContext Create(ConsumeContext consumeContext);
}

internal sealed class DefaultSagaContextFactory : ISagaContextFactory
{
    public ISagaContext Create(ConsumeContext consumeContext)
    {
        var bus = consumeContext.Services.GetRequiredService<IBus>();
        return new SagaContextImpl(bus);
    }
}
```

---

## AvtoBus.Core/Dispatching/AvtoBusRegistry.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus.Dispatching;

/// <summary>
/// Статический реестр диспетчеров, заполняемый через [ModuleInitializer]
/// сгенерированным кодом. Работает до построения DI-контейнера.
/// </summary>
public static class AvtoBusRegistry
{
    private static readonly ConcurrentBag<IMessageDispatcher> _dispatchers = new();
    private static readonly ConcurrentDictionary<Type, string> _typeAliases = new();

    /// <summary>
    /// Вызывается сгенерированным ModuleInitializer.
    /// </summary>
    public static void Register(IMessageDispatcher dispatcher)
    {
        _dispatchers.Add(dispatcher);
        _typeAliases[dispatcher.ClrType] = dispatcher.MessageType;
    }

    /// <summary>
    /// Зарегистрировать алиас типа без диспетчера (для контрактов-только).
    /// </summary>
    public static void RegisterAlias(Type clrType, string messageType)
        => _typeAliases[clrType] = messageType;

    public static IReadOnlyCollection<IMessageDispatcher> GetAll() => _dispatchers.ToArray();

    public static IReadOnlyDictionary<Type, string> GetAliases() => _typeAliases;

    public static bool IsEmpty => _dispatchers.IsEmpty;

    /// <summary>Только для тестов.</summary>
    internal static void Clear()
    {
        _dispatchers.Clear();
        _typeAliases.Clear();
    }
}
```

---

## AvtoBus.Core/Dispatching/ReflectionDispatcherBuilder.cs

```csharp
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Dispatching;

/// <summary>
/// Fallback-построитель диспетчеров через рефлексию.
/// Используется, если Source Generator не подключён (например, в тестах или динамической загрузке).
/// В продакшене предпочтителен codegen — он быстрее и AOT-совместим.
/// </summary>
public static class ReflectionDispatcherBuilder
{
    public static IEnumerable<IMessageDispatcher> BuildFromAssemblies(
        IEnumerable<Assembly> assemblies,
        IServiceCollection services)
    {
        foreach (var assembly in assemblies)
        {
            foreach (var dispatcher in BuildFromAssembly(assembly, services))
                yield return dispatcher;
        }
    }

    public static IEnumerable<IMessageDispatcher> BuildFromAssembly(
        Assembly assembly,
        IServiceCollection services)
    {
        var types = assembly.GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false });

        foreach (var type in types)
        {
            // 1. IConsumer<T>
            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType) continue;
                var def = iface.GetGenericTypeDefinition();

                if (def == typeof(IConsumer<>))
                {
                    var messageType = iface.GetGenericArguments()[0];
                    services.AddScoped(type);
                    yield return CreateConsumerDispatcher(type, messageType);
                }
                else if (def == typeof(IHandleMessages<>))
                {
                    var messageType = iface.GetGenericArguments()[0];
                    services.AddScoped(type);
                    yield return CreateHandleMessagesDispatcher(type, messageType);
                }
            }

            // 2. Статические/инстанс методы Handle/Consume
            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(m => m.Name is "Handle" or "Consume")
                .Where(m => m.GetParameters().Length > 0)
                .Where(m => IsMessageParameter(m.GetParameters()[0].ParameterType));

            foreach (var method in methods)
            {
                var messageType = method.GetParameters()[0].ParameterType;
                if (!method.IsStatic)
                    services.AddScoped(type);
                yield return CreateMethodDispatcher(type, method, messageType);
            }
        }
    }

    public static IEnumerable<IMessageDispatcher> BuildFromTypes(
        IEnumerable<Type> types,
        IServiceCollection services)
    {
        foreach (var type in types)
        {
            foreach (var iface in type.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IConsumer<>))
                {
                    var messageType = iface.GetGenericArguments()[0];
                    services.AddScoped(type);
                    yield return CreateConsumerDispatcher(type, messageType);
                }
            }
        }
    }

    private static bool IsMessageParameter(Type t) =>
        t is { IsClass: true, IsAbstract: false }
        && t != typeof(string)
        && t != typeof(CancellationToken)
        && !t.Name.StartsWith("IService")
        && !typeof(Delegate).IsAssignableFrom(t);

    // ── Фабрики диспетчеров ──

    private static IMessageDispatcher CreateConsumerDispatcher(Type consumerType, Type messageType)
    {
        var dispatcherType = typeof(ConsumerDispatcher<,>).MakeGenericType(consumerType, messageType);
        return (IMessageDispatcher)Activator.CreateInstance(dispatcherType)!;
    }

    private static IMessageDispatcher CreateHandleMessagesDispatcher(Type handlerType, Type messageType)
    {
        var dispatcherType = typeof(HandleMessagesDispatcher<,>).MakeGenericType(handlerType, messageType);
        return (IMessageDispatcher)Activator.CreateInstance(dispatcherType)!;
    }

    private static IMessageDispatcher CreateMethodDispatcher(Type declaringType, MethodInfo method, Type messageType)
        => new MethodDispatcher(declaringType, method, messageType);
}

/// <summary>Диспетчер для IConsumer&lt;T&gt;.</summary>
internal sealed class ConsumerDispatcher<TConsumer, TMessage> : IMessageDispatcher
    where TConsumer : class, IConsumer<TMessage>
    where TMessage : class
{
    public string MessageType => MessageTypeNaming.For(typeof(TMessage));
    public Type ClrType => typeof(TMessage);

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var consumer = context.Services.GetRequiredService<TConsumer>();
        var typed = new ConsumeContext<TMessage>
        {
            Envelope = context.Envelope,
            Message = (TMessage)context.Message,
            Services = context.Services,
            CancellationToken = context.CancellationToken,
        };
        await consumer.Consume(typed);

        // Перенести исходящие из типизированного контекста в родительский
        foreach (var outgoing in typed.Outgoing)
            context.AddOutgoing(outgoing);
    }
}

/// <summary>Диспетчер для IHandleMessages&lt;T&gt;.</summary>
internal sealed class HandleMessagesDispatcher<THandler, TMessage> : IMessageDispatcher
    where THandler : class, IHandleMessages<TMessage>
    where TMessage : class
{
    public string MessageType => MessageTypeNaming.For(typeof(TMessage));
    public Type ClrType => typeof(TMessage);

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var handler = context.Services.GetRequiredService<THandler>();
        await handler.Handle((TMessage)context.Message);
    }
}

/// <summary>Диспетчер для метода Handle/Consume (рефлексионный fallback).</summary>
internal sealed class MethodDispatcher : IMessageDispatcher
{
    private readonly Type _declaringType;
    private readonly MethodInfo _method;
    private readonly ParameterInfo[] _parameters;
    private readonly bool _isStatic;
    private readonly bool _returnsTask;
    private readonly bool _returnsValueTask;
    private readonly bool _hasReturnValue;

    public MethodDispatcher(Type declaringType, MethodInfo method, Type messageType)
    {
        _declaringType = declaringType;
        _method = method;
        _parameters = method.GetParameters();
        _isStatic = method.IsStatic;
        ClrType = messageType;
        MessageType = MessageTypeNaming.For(messageType);

        var rt = method.ReturnType;
        _returnsTask = typeof(Task).IsAssignableFrom(rt);
        _returnsValueTask = rt == typeof(ValueTask) || (rt.IsGenericType && rt.GetGenericTypeDefinition() == typeof(ValueTask<>));
        _hasReturnValue = rt != typeof(void) && rt != typeof(Task) && rt != typeof(ValueTask);
    }

    public string MessageType { get; }
    public Type ClrType { get; }

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        // Резолв аргументов
        var args = new object?[_parameters.Length];
        args[0] = context.Message;

        for (var i = 1; i < _parameters.Length; i++)
        {
            var p = _parameters[i];
            args[i] = p.ParameterType switch
            {
                var t when t == typeof(CancellationToken) => context.CancellationToken,
                var t when t == typeof(ConsumeContext) => context,
                var t when t == typeof(IServiceProvider) => context.Services,
                var t when t == typeof(Envelope) => context.Envelope,
                var t => context.Services.GetService(t)
                         ?? (p.HasDefaultValue ? p.DefaultValue : throw new InvalidOperationException(
                             $"Cannot resolve '{t.Name}' for handler {_declaringType.Name}.{_method.Name}"))
            };
        }

        var target = _isStatic ? null : context.Services.GetRequiredService(_declaringType);
        var result = _method.Invoke(target, args);

        // Await результата
        object? value = result;
        if (_returnsTask && result is Task task)
        {
            await task.ConfigureAwait(false);
            value = _hasReturnValue ? GetTaskResult(task) : null;
        }
        else if (_returnsValueTask && result is not null)
        {
            value = await AwaitValueTask(result);
        }

        // Каскадная публикация возвращённого значения
        if (value is not null)
            await CascadeAsync(context, value);
    }

    private static object? GetTaskResult(Task task)
    {
        var type = task.GetType();
        return type.IsGenericType ? type.GetProperty("Result")?.GetValue(task) : null;
    }

    private static async ValueTask<object?> AwaitValueTask(object valueTask)
    {
        var type = valueTask.GetType();
        var asTask = type.GetMethod("AsTask")!;
        var task = (Task)asTask.Invoke(valueTask, null)!;
        await task.ConfigureAwait(false);
        return GetTaskResult(task);
    }

    private static async ValueTask CascadeAsync(ConsumeContext context, object value)
    {
        switch (value)
        {
            case OutgoingMessages outgoing:
                foreach (var item in outgoing.Items)
                    context.AddOutgoing(item);
                break;

            case System.Runtime.CompilerServices.ITuple tuple:
                for (var i = 0; i < tuple.Length; i++)
                    if (tuple[i] is { } item)
                        await context.PublishAsync(item);
                break;

            case System.Collections.IEnumerable seq and not string:
                foreach (var item in seq)
                    if (item is not null)
                        await context.PublishAsync(item);
                break;

            default:
                await context.PublishAsync(value);
                break;
        }
    }
}
```

---

## AvtoBus.Core/MessageTypeNaming.cs

```csharp
using System.Collections.Concurrent;
using System.Text;

namespace AvtoBus;

/// <summary>
/// Соглашение об именовании типов сообщений.
/// OrderPlaced → "order-placed"; [MessageAlias] переопределяет.
/// </summary>
public static class MessageTypeNaming
{
    private static readonly ConcurrentDictionary<Type, string> _cache = new();

    public static string For(Type type) => _cache.GetOrAdd(type, static t =>
    {
        var alias = t.GetCustomAttributes(typeof(MessageAliasAttribute), inherit: false)
            .OfType<MessageAliasAttribute>()
            .FirstOrDefault();

        if (alias is not null)
            return alias.Name;

        return ToKebabCase(t.Name);
    });

    public static string ToKebabCase(string name)
    {
        var sb = new StringBuilder(name.Length + 8);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0) sb.Append('-');
                sb.Append(char.ToLowerInvariant(c));
            }
            else sb.Append(c);
        }
        return sb.ToString();
    }
}

/// <summary>
/// Явное имя типа сообщения на проводе (стабильное при рефакторинге классов).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = true)]
public sealed class MessageAliasAttribute : Attribute
{
    public string Name { get; }
    public string[] LegacyNames { get; }

    public MessageAliasAttribute(string name, params string[] legacyNames)
    {
        Name = name;
        LegacyNames = legacyNames;
    }
}

/// <summary>Транспортные хинты в контракте.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class MessageAttribute : Attribute
{
    public bool Durable { get; set; } = true;
    public byte Priority { get; set; } = 4;
    public string? Ttl { get; set; }
    public string? Topic { get; set; }
    public string? Queue { get; set; }
}

/// <summary>Поле — ключ партиции.</summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class PartitionKeyAttribute : Attribute;

/// <summary>Персональные данные: маскирование + шифрование.</summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class PersonalDataAttribute : Attribute;

/// <summary>Идемпотентная обработка по ключу.</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class IdempotentAttribute : Attribute
{
    public string? Key { get; set; }
    public string Window { get; set; } = "24:00:00";
}

/// <summary>Таймаут обработки хендлера.</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class HandlerTimeoutAttribute(string timeout) : Attribute
{
    public TimeSpan Timeout { get; } = TimeSpan.Parse(timeout);
}
```

---

## AvtoBus.Core/Subscriptions/ISubscriptionCatalog.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Каталог подписок: что слушать, откуда, с какими настройками.
/// </summary>
public interface ISubscriptionCatalog
{
    IReadOnlyList<SubscriptionEntry> Subscriptions { get; }
}

public sealed record SubscriptionEntry
{
    public required string ConsumerId { get; init; }
    public required string TransportName { get; init; }
    public required string Destination { get; init; }
    public IReadOnlyList<string> Topics { get; init; } = Array.Empty<string>();
    public int Prefetch { get; init; } = 32;
    public int MaxParallelism { get; init; } = Environment.ProcessorCount;
    public IReadOnlyList<Type> MessageTypes { get; init; } = Array.Empty<Type>();

    public TransportSubscription ToTransportSubscription()
        => new(Destination, Topics, Prefetch, ConsumerId);
}

/// <summary>
/// Строит каталог из зарегистрированных диспетчеров + правил маршрутизации.
/// </summary>
internal sealed class ConventionSubscriptionCatalog : ISubscriptionCatalog
{
    public IReadOnlyList<SubscriptionEntry> Subscriptions { get; }

    public ConventionSubscriptionCatalog(
        DispatcherRegistry registry,
        IRouter router,
        BusOptions options)
    {
        var entries = new Dictionary<string, SubscriptionEntry>();

        foreach (var dispatcher in registry.All)
        {
            var isCommand = typeof(ICommand).IsAssignableFrom(dispatcher.ClrType);
            var route = router.Route(dispatcher.ClrType, isCommand);
            var queue = isCommand
                ? route.Destination.Address
                : $"{options.EndpointName}.{route.Destination.Address}";

            if (entries.TryGetValue(queue, out var existing))
            {
                entries[queue] = existing with
                {
                    Topics = isCommand
                        ? existing.Topics
                        : existing.Topics.Append(route.Destination.Address).Distinct().ToList(),
                    MessageTypes = existing.MessageTypes.Append(dispatcher.ClrType).ToList(),
                };
            }
            else
            {
                entries[queue] = new SubscriptionEntry
                {
                    ConsumerId = queue,
                    TransportName = route.Transport,
                    Destination = queue,
                    Topics = isCommand ? [] : [route.Destination.Address],
                    Prefetch = options.DefaultPrefetch,
                    MaxParallelism = options.DefaultMaxParallelism,
                    MessageTypes = [dispatcher.ClrType],
                };
            }
        }

        Subscriptions = entries.Values.ToList();
    }
}
```

---

## AvtoBus.Core/Internal/ObjectPool.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus.Internal;

/// <summary>
/// Минимальный пул объектов (чтобы не тянуть Microsoft.Extensions.ObjectPool).
/// </summary>
public abstract class PooledObjectPolicy<T> where T : class
{
    public abstract T Create();
    /// <summary>Вернуть true, если объект можно переиспользовать.</summary>
    public abstract bool Return(T obj);
}

public sealed class ObjectPool<T> : IDisposable where T : class
{
    private readonly ConcurrentBag<T> _items = new();
    private readonly PooledObjectPolicy<T> _policy;
    private readonly int _maxRetained;
    private int _count;

    public ObjectPool(PooledObjectPolicy<T> policy, int maxRetained = 32)
    {
        _policy = policy;
        _maxRetained = maxRetained;
    }

    public T Get()
    {
        if (_items.TryTake(out var item))
        {
            Interlocked.Decrement(ref _count);
            return item;
        }
        return _policy.Create();
    }

    public void Return(T item)
    {
        if (!_policy.Return(item)) return;
        if (Interlocked.Increment(ref _count) > _maxRetained)
        {
            Interlocked.Decrement(ref _count);
            (item as IDisposable)?.Dispose();
            return;
        }
        _items.Add(item);
    }

    public void Dispose()
    {
        while (_items.TryTake(out var item))
            (item as IDisposable)?.Dispose();
    }
}
```

---

## AvtoBus.Core/Chaos/ChaosOptions.cs

```csharp
using AvtoBus.Pipeline;

namespace AvtoBus.Chaos;

/// <summary>
/// Хаос-инъекции для проверки надёжности (только не-прод окружения).
/// </summary>
public sealed class ChaosOptions
{
    public double DuplicateProbability { get; set; }
    public double DropProbability { get; set; }
    public double FailProbability { get; set; }
    public TimeSpan DelayJitter { get; set; } = TimeSpan.Zero;
    public double ReorderProbability { get; set; }
    public int? Seed { get; set; }
}

public sealed class ChaosMiddleware : IBusMiddleware
{
    private readonly ChaosOptions _options;
    private readonly Random _random;
    private readonly ILogger<ChaosMiddleware> _log;

    public ChaosMiddleware(ChaosOptions options, ILogger<ChaosMiddleware> log)
    {
        _options = options;
        _log = log;
        _random = options.Seed is { } seed ? new Random(seed) : Random.Shared;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (_options.DropProbability > 0 && _random.NextDouble() < _options.DropProbability)
        {
            _log.LogWarning("CHAOS: dropped {Type}", ctx.Envelope.MessageType);
            return;
        }

        if (_options.DelayJitter > TimeSpan.Zero)
        {
            var delay = TimeSpan.FromMilliseconds(_random.NextDouble() * _options.DelayJitter.TotalMilliseconds);
            await Task.Delay(delay, ctx.CancellationToken);
        }

        if (_options.FailProbability > 0 && _random.NextDouble() < _options.FailProbability)
        {
            _log.LogWarning("CHAOS: injected failure for {Type}", ctx.Envelope.MessageType);
            throw new ChaosInjectedException(ctx.Envelope.MessageType);
        }

        await next(ctx);

        if (_options.DuplicateProbability > 0 && _random.NextDouble() < _options.DuplicateProbability)
        {
            _log.LogWarning("CHAOS: duplicating {Type}", ctx.Envelope.MessageType);
            await next(ctx);
        }
    }
}

public sealed class ChaosInjectedException(string messageType)
    : Exception($"Chaos injection for {messageType}");
```

---

## AvtoBus.Core/IOutboxStatus.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Состояние outbox для дашборда и health-checks.
/// </summary>
public interface IOutboxStatus
{
    int PendingCount { get; }
    int FailedCount { get; }
    DateTimeOffset? OldestPendingAt { get; }
    DateTimeOffset? LastRelayRunAt { get; }
}

internal sealed class OutboxStatusSnapshot : IOutboxStatus
{
    public int PendingCount { get; set; }
    public int FailedCount { get; set; }
    public DateTimeOffset? OldestPendingAt { get; set; }
    public DateTimeOffset? LastRelayRunAt { get; set; }
}
```

---

## AvtoBus.Core/HealthChecks/BusHealthCheck.cs

```csharp
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AvtoBus.HealthChecks;

/// <summary>
/// Health-check шины: транспорт, outbox, лаг консьюмеров.
/// </summary>
public sealed class BusHealthCheck : IHealthCheck
{
    private readonly ITransportSelector _transports;
    private readonly IOutboxStatus? _outbox;
    private readonly BusHealthOptions _options;

    public BusHealthCheck(
        ITransportSelector transports,
        BusHealthOptions options,
        IOutboxStatus? outbox = null)
    {
        _transports = transports;
        _options = options;
        _outbox = outbox;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var data = new Dictionary<string, object>();
        var issues = new List<string>();

        if (_outbox is not null)
        {
            data["outbox.pending"] = _outbox.PendingCount;
            data["outbox.failed"] = _outbox.FailedCount;

            if (_outbox.PendingCount > _options.MaxOutboxPending)
                issues.Add($"Outbox backlog {_outbox.PendingCount} > {_options.MaxOutboxPending}");

            if (_outbox.OldestPendingAt is { } oldest)
            {
                var age = DateTimeOffset.UtcNow - oldest;
                data["outbox.oldest_age_seconds"] = age.TotalSeconds;
                if (age > _options.MaxOutboxAge)
                    issues.Add($"Oldest outbox message age {age:g} > {_options.MaxOutboxAge:g}");
            }
        }

        return Task.FromResult(issues.Count == 0
            ? HealthCheckResult.Healthy("AvtoBus healthy", data)
            : HealthCheckResult.Degraded(string.Join("; ", issues), data: data));
    }
}

public sealed class BusHealthOptions
{
    public int MaxOutboxPending { get; set; } = 10_000;
    public TimeSpan MaxOutboxAge { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan MaxConsumerLag { get; set; } = TimeSpan.FromMinutes(2);
}
```

---

## Дополнения к существующим типам

```csharp
// DispatcherRegistry — добавить перечисление всех
public sealed partial class DispatcherRegistry
{
    public IEnumerable<IMessageDispatcher> All => _byType.Values;
}

// BusOptions — недостающие поля, на которые ссылается ConventionSubscriptionCatalog
public sealed partial class BusOptions
{
    public string EndpointName { get; set; } = Assembly.GetEntryAssembly()?.GetName().Name?.ToLowerInvariant() ?? "avtobus";
    public int DefaultPrefetch { get; set; } = 32;
    public int DefaultMaxParallelism { get; set; } = Environment.ProcessorCount;
    public bool HasUnitOfWork { get; set; }
}

// ConsumeContext — флаг наличия UoW (используется DefaultBus для outbox)
public partial class ConsumeContext
{
    public bool HasUnitOfWork => Items.ContainsKey("AvtoBus.UnitOfWork");
    public void MarkUnitOfWork() => Items["AvtoBus.UnitOfWork"] = true;
}
```
