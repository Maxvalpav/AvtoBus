# AvtoBus.Core — Маркеры, сериализатор, каталог, диспетчеры

Недостающие базовые куски, без которых остальной код не компилируется.

---

## AvtoBus.Core/Markers.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Маркер команды — ровно один получатель, отправляется через Send.
/// </summary>
public interface ICommand;

/// <summary>
/// Маркер события — 0..N подписчиков, публикуется через Publish.
/// </summary>
public interface IEvent;

/// <summary>
/// Маркер запроса для Request/Response.
/// </summary>
public interface IRequest<TReply> where TReply : class;

/// <summary>
/// Интерфейсный консьюмер (Rebus/MassTransit-style).
/// </summary>
public interface IConsumer<in TMessage> where TMessage : class
{
    Task Consume(ConsumeContext<TMessage> context);
}

/// <summary>
/// Батч-консьюмер (Broadway-style).
/// </summary>
public interface IBatchConsumer<TMessage> where TMessage : class
{
    Task Consume(IMessageBatch<TMessage> batch);
}

public interface IMessageBatch<out TMessage> where TMessage : class
{
    IReadOnlyList<TMessage> Messages { get; }
    int Count { get; }
}

/// <summary>
/// Обработчик проваленного сообщения (second-level retry).
/// </summary>
public interface IFailed<out TMessage> where TMessage : class
{
    TMessage Message { get; }
    string ErrorDescription { get; }
    Exception Exception { get; }
    int Attempts { get; }
}
```

---

## AvtoBus.Sagas/SagaMarkers.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Сообщение начинает новый инстанс саги.
/// </summary>
public interface IStartedBy<in TMessage> where TMessage : class
{
    Task Handle(TMessage message);
}

/// <summary>
/// Сообщение обрабатывается существующим инстансом саги.
/// </summary>
public interface IHandle<in TMessage> where TMessage : class
{
    Task Handle(TMessage message);
}
```

---

## AvtoBus.Core/Attributes.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Явное имя типа сообщения для провода (стабильное при рефакторинге).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class MessageAliasAttribute : Attribute
{
    public string PrimaryName { get; }
    public string[] LegacyNames { get; }

    public MessageAliasAttribute(string primaryName, params string[] legacyNames)
    {
        PrimaryName = primaryName;
        LegacyNames = legacyNames;
    }
}

/// <summary>
/// Явный топик/очередь для сообщения.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class TopicAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

/// <summary>
/// Ключ партиционирования — свойство сообщения.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class PartitionKeyAttribute : Attribute;

/// <summary>
/// Приоритет сообщения.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class PriorityAttribute(byte priority) : Attribute
{
    public byte Priority { get; } = priority;
}

/// <summary>
/// Персональные данные — маскируются в логах, шифруются в сторе.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class PersonalDataAttribute : Attribute;

/// <summary>
/// Идемпотентная обработка по ключу.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class IdempotentAttribute : Attribute
{
    public string? Key { get; set; }
    public string Window { get; set; } = "24:00:00";
}

/// <summary>
/// Таймаут обработки хендлера.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class HandlerTimeoutAttribute(string timeout) : Attribute
{
    public TimeSpan Timeout { get; } = TimeSpan.Parse(timeout);
}

/// <summary>
/// SLA для саги.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class SagaSlaAttribute : Attribute
{
    public Type From { get; set; } = null!;
    public Type To { get; set; } = null!;
    public string MaxDuration { get; set; } = "01:00:00";
}
```

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
    object Deserialize(ReadOnlyMemory<byte> body, Type type);
    T Deserialize<T>(ReadOnlyMemory<byte> body) where T : class;
}
```

---

## AvtoBus.Core/Serialization/DefaultJsonSerializer.cs

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AvtoBus.Serialization;

/// <summary>
/// System.Text.Json сериализация с source-generation поддержкой.
/// </summary>
public sealed class DefaultJsonSerializer : ISerializer
{
    public string ContentType => "application/json";

    private readonly JsonSerializerOptions _options;

    public DefaultJsonSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
            PropertyNameCaseInsensitive = true,
        };
    }

    public ReadOnlyMemory<byte> Serialize(object message)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), _options);
    }

    public object Deserialize(ReadOnlyMemory<byte> body, Type type)
    {
        var result = JsonSerializer.Deserialize(body.Span, type, _options);
        return result ?? throw new SerializationException(
            $"Failed to deserialize {type.Name}: result was null");
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> body) where T : class
    {
        return JsonSerializer.Deserialize<T>(body.Span, _options)
            ?? throw new SerializationException(
                $"Failed to deserialize {typeof(T).Name}: result was null");
    }
}

public sealed class SerializationException(string message) : Exception(message);
```

---

## AvtoBus.Core/Serialization/MessagePackSerializer.cs

```csharp
namespace AvtoBus.Serialization;

/// <summary>
/// MessagePack сериализация (компактнее и быстрее JSON).
/// Требует пакет AvtoBus.Serialization.MessagePack.
/// </summary>
public sealed class MessagePackBusSerializer : ISerializer
{
    public string ContentType => "application/x-msgpack";

    private readonly global::MessagePack.MessagePackSerializerOptions _options;

    public MessagePackBusSerializer(global::MessagePack.MessagePackSerializerOptions? options = null)
    {
        _options = options ?? global::MessagePack.MessagePackSerializerOptions.Standard
            .WithCompression(global::MessagePack.MessagePackCompression.Lz4BlockArray);
    }

    public ReadOnlyMemory<byte> Serialize(object message)
        => global::MessagePack.MessagePackSerializer.Serialize(message.GetType(), message, _options);

    public object Deserialize(ReadOnlyMemory<byte> body, Type type)
        => global::MessagePack.MessagePackSerializer.Deserialize(type, body, _options)!;

    public T Deserialize<T>(ReadOnlyMemory<byte> body) where T : class
        => global::MessagePack.MessagePackSerializer.Deserialize<T>(body, _options)!;
}
```

---

## AvtoBus.Core/Subscription/ISubscriptionCatalog.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Каталог подписок — какие очереди слушаем, какие типы обрабатываем.
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
    public IReadOnlyList<Type> MessageTypes { get; init; } = Array.Empty<Type>();
}
```

---

## AvtoBus.Core/Subscription/ReflectionSubscriptionCatalog.cs

```csharp
using AvtoBus.Dispatching;

namespace AvtoBus;

/// <summary>
/// Строит каталог подписок из зарегистрированных диспетчеров.
/// Convention: команда → своя очередь, событие → топик.
/// </summary>
internal sealed class ReflectionSubscriptionCatalog : ISubscriptionCatalog
{
    public IReadOnlyList<SubscriptionEntry> Subscriptions { get; }

    public ReflectionSubscriptionCatalog(
        DispatcherRegistry registry,
        IRouter router,
        ITypeResolver typeResolver,
        BusOptions options)
    {
        var entries = new List<SubscriptionEntry>();

        // Группируем диспетчеры: команды по своим очередям, события собираем в одну очередь сервиса
        var commandDispatchers = new List<IMessageDispatcher>();
        var eventDispatchers = new List<IMessageDispatcher>();

        foreach (var dispatcher in EnumerateDispatchers(registry))
        {
            var isCommand = typeof(ICommand).IsAssignableFrom(dispatcher.ClrType);
            if (isCommand)
                commandDispatchers.Add(dispatcher);
            else
                eventDispatchers.Add(dispatcher);
        }

        // Каждая команда — своя очередь
        foreach (var dispatcher in commandDispatchers)
        {
            var route = router.Route(dispatcher.ClrType, isCommand: true);
            entries.Add(new SubscriptionEntry
            {
                ConsumerId = $"{route.Destination.Address}-consumer",
                TransportName = options.DefaultTransport,
                Destination = route.Destination.Address,
                Topics = Array.Empty<string>(),
                Prefetch = 32,
                MessageTypes = new[] { dispatcher.ClrType },
            });
        }

        // Все события сервиса — в одну очередь этого сервиса, подписанную на топики
        if (eventDispatchers.Count > 0)
        {
            var serviceQueue = $"{AppDomain.CurrentDomain.FriendlyName}-events".ToLowerInvariant();
            var topics = eventDispatchers
                .Select(d => router.Route(d.ClrType, isCommand: false).Destination.Address)
                .Distinct()
                .ToList();

            entries.Add(new SubscriptionEntry
            {
                ConsumerId = serviceQueue,
                TransportName = options.DefaultTransport,
                Destination = serviceQueue,
                Topics = topics,
                Prefetch = 32,
                MessageTypes = eventDispatchers.Select(d => d.ClrType).ToList(),
            });
        }

        Subscriptions = entries;
    }

    private static IEnumerable<IMessageDispatcher> EnumerateDispatchers(DispatcherRegistry registry)
    {
        // registry предоставляет доступ к своим диспетчерам через internal-метод
        return registry.All;
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
/// Fallback-построитель диспетчеров через рефлексию
/// (когда Source Generator не задействован, например в тестах).
/// </summary>
internal static class ReflectionDispatcherBuilder
{
    public static IEnumerable<IMessageDispatcher> BuildFromAssemblies(
        IEnumerable<Assembly> assemblies,
        IServiceProvider services)
    {
        foreach (var assembly in assemblies)
        {
            foreach (var dispatcher in BuildFromAssembly(assembly, services))
                yield return dispatcher;
        }
    }

    public static IEnumerable<IMessageDispatcher> BuildFromTypes(
        IEnumerable<Type> types,
        IServiceProvider services)
    {
        foreach (var type in types)
        {
            foreach (var dispatcher in BuildFromType(type, services))
                yield return dispatcher;
        }
    }

    private static IEnumerable<IMessageDispatcher> BuildFromAssembly(
        Assembly assembly, IServiceProvider services)
    {
        var types = assembly.GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false });

        foreach (var type in types)
        {
            foreach (var d in BuildFromType(type, services))
                yield return d;
        }
    }

    private static IEnumerable<IMessageDispatcher> BuildFromType(
        Type type, IServiceProvider services)
    {
        // 1. Интерфейсные консьюмеры: IConsumer<T>
        var consumerInterfaces = type.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>));

        foreach (var iface in consumerInterfaces)
        {
            var messageType = iface.GetGenericArguments()[0];
            yield return new InterfaceConsumerDispatcher(type, messageType);
        }

        // 2. Method-handlers: static/instance Handle/Consume
        var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.Name is "Handle" or "Consume" && m.GetParameters().Length >= 1);

        foreach (var method in methods)
        {
            var firstParam = method.GetParameters()[0].ParameterType;
            if (firstParam == typeof(CancellationToken)) continue;
            if (!firstParam.IsClass) continue;

            yield return new MethodHandlerDispatcher(type, method, firstParam);
        }
    }
}

/// <summary>
/// Диспетчер интерфейсного консьюмера через рефлексию.
/// </summary>
internal sealed class InterfaceConsumerDispatcher : IMessageDispatcher
{
    private readonly Type _consumerType;
    private readonly Type _messageType;
    private readonly MethodInfo _consumeMethod;

    public InterfaceConsumerDispatcher(Type consumerType, Type messageType)
    {
        _consumerType = consumerType;
        _messageType = messageType;
        var ctxType = typeof(ConsumeContext<>).MakeGenericType(messageType);
        _consumeMethod = consumerType.GetMethod("Consume", new[] { ctxType })!;
    }

    public string MessageType => TypeNaming.ToKebab(_messageType.Name);
    public Type ClrType => _messageType;

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var consumer = context.Services.GetRequiredService(_consumerType);

        var typedCtx = Activator.CreateInstance(
            typeof(ConsumeContext<>).MakeGenericType(_messageType));
        // Заполняем типизированный контекст (упрощённо)

        var result = _consumeMethod.Invoke(consumer, new[] { context });
        if (result is Task task) await task;
    }
}

/// <summary>
/// Диспетчер method-handler через рефлексию.
/// </summary>
internal sealed class MethodHandlerDispatcher : IMessageDispatcher
{
    private readonly Type _containingType;
    private readonly MethodInfo _method;
    private readonly Type _messageType;
    private readonly ParameterInfo[] _parameters;
    private readonly bool _isStatic;
    private readonly bool _returnsCascade;

    public MethodHandlerDispatcher(Type containingType, MethodInfo method, Type messageType)
    {
        _containingType = containingType;
        _method = method;
        _messageType = messageType;
        _parameters = method.GetParameters();
        _isStatic = method.IsStatic;
        _returnsCascade = method.ReturnType != typeof(void)
            && method.ReturnType != typeof(Task)
            && method.ReturnType != typeof(ValueTask);
    }

    public string MessageType => TypeNaming.ToKebab(_messageType.Name);
    public Type ClrType => _messageType;

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var args = new object?[_parameters.Length];
        args[0] = context.Message;

        for (int i = 1; i < _parameters.Length; i++)
        {
            var pType = _parameters[i].ParameterType;
            if (pType == typeof(CancellationToken))
                args[i] = context.CancellationToken;
            else if (pType == typeof(ConsumeContext))
                args[i] = context;
            else
                args[i] = context.Services.GetRequiredService(pType);
        }

        object? target = _isStatic
            ? null
            : context.Services.GetService(_containingType)
              ?? ActivatorUtilities.CreateInstance(context.Services, _containingType);

        var result = _method.Invoke(target, args);

        object? cascadeResult = null;
        switch (result)
        {
            case Task task:
                await task;
                if (_returnsCascade)
                    cascadeResult = task.GetType().GetProperty("Result")?.GetValue(task);
                break;
            case ValueTask valueTask:
                await valueTask;
                break;
            default:
                cascadeResult = result;
                break;
        }

        // Каскадные сообщения
        if (cascadeResult is not null && _returnsCascade)
            await PublishCascade(context, cascadeResult);
    }

    private static async ValueTask PublishCascade(ConsumeContext context, object result)
    {
        // Кортеж → несколько сообщений
        if (result is System.Runtime.CompilerServices.ITuple tuple)
        {
            for (int i = 0; i < tuple.Length; i++)
            {
                var item = tuple[i];
                if (item is not null)
                    await context.PublishAsync(item);
            }
            return;
        }

        // OutgoingMessages
        if (result is OutgoingMessages outgoing)
        {
            var bus = context.Services.GetRequiredService<IBus>();
            await outgoing.ApplyAsync(bus, context, context.CancellationToken);
            return;
        }

        // Одно сообщение
        await context.PublishAsync(result);
    }
}

/// <summary>
/// Утилита именования типов.
/// </summary>
internal static class TypeNaming
{
    public static string ToKebab(string name)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i]))
                sb.Append('-');
            sb.Append(char.ToLowerInvariant(name[i]));
        }
        return sb.ToString();
    }
}
```

---

## AvtoBus.Core/AvtoBusRegistry.cs

```csharp
using System.Collections.Concurrent;
using AvtoBus.Dispatching;

namespace AvtoBus;

/// <summary>
/// Глобальный реестр диспетчеров, заполняемый Source Generator'ом
/// через [ModuleInitializer].
/// </summary>
public static class AvtoBusRegistry
{
    private static readonly ConcurrentBag<IMessageDispatcher> _dispatchers = new();

    public static void Register(IMessageDispatcher dispatcher)
        => _dispatchers.Add(dispatcher);

    public static IReadOnlyCollection<IMessageDispatcher> All => _dispatchers.ToArray();

    internal static void Clear() => _dispatchers.Clear();
}
```

Дополнение к `DispatcherRegistry` (свойство `All`):

```csharp
// Добавить в AvtoBus.Core/Dispatching/DispatcherRegistry.cs
public sealed class DispatcherRegistry
{
    private readonly IReadOnlyList<IMessageDispatcher> _all;
    // ... существующие поля ...

    public IReadOnlyList<IMessageDispatcher> All => _all;

    public DispatcherRegistry(IEnumerable<IMessageDispatcher> dispatchers)
    {
        // Объединяем то, что нашёл рефлексионный билдер, и то, что зарегистрировал генератор
        var list = dispatchers.Concat(AvtoBusRegistry.All)
            .GroupBy(d => d.MessageType)
            .Select(g => g.First())
            .ToArray();

        _all = list;
        _byType = list.ToFrozenDictionary(d => d.MessageType, StringComparer.OrdinalIgnoreCase);
        _byClr = list.ToFrozenDictionary(d => d.ClrType);
    }
}
```
