using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization;
using AvtoBus.Handlers;
using AvtoBus.Pipeline;
using AvtoBus.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Configuration;

public sealed partial class BusConfigurator
{
    // ---- Хендлеры -------------------------------------------------------

    /// <summary>
    /// Каноническая регистрация хендлеров: находит в сборке статические методы <c>Handle</c>
    /// (канонический стиль AvtoBus) и классы <see cref="IConsumer{T}"/> (стиль с DI-зависимостями).
    /// </summary>
    /// <remarks>Сканирование сборки через рефлексию — legacy-режим: несовместимо с trimming/AOT.
    /// Под AOT регистрируйте хендлеры явно через <c>AddConsumer&lt;T&gt;</c> с подключённым генератором.</remarks>
    [RequiresUnreferencedCode(
        "Сканирование сборки на хендлеры несовместимо с trimming. Под AOT регистрируйте хендлеры через " +
        "AddConsumer<T>() с подключённым AvtoBus.Generators.")]
    public BusConfigurator AddConsumersFromAssembly(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (type.IsAbstract && !type.IsSealed)
                continue;

            if (type.IsGenericTypeDefinition)
                continue;

            AddConsumerType(type);
        }

        return this;
    }

    public BusConfigurator AddConsumersFromAssemblyContaining<T>()
        => AddConsumersFromAssembly(typeof(T).Assembly);

    /// <summary>AOT-safe: регистрирует только сгенерированные диспетчеры (без рефлексии).</summary>
    public BusConfigurator AddConsumersFromGenerated()
    {
        foreach (var type in AvtoBus.Dispatching.AvtoBusRegistry.GeneratedHandlerTypes)
            AddGeneratedDispatchers(type);
        return this;
    }

    /// <summary>Регистрирует один конкретный тип-хендлер.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Под AOT тип обязан быть покрыт генератором: тогда работает только сгенерированный диспетчер " +
        "(AvtoBusRegistry.HasGeneratedFor), а reflection-ветка недостижима. Reflection-fallback — legacy-режим " +
        "без генератора; trimming без генератора не поддерживается (док 16 §8).")]
    public BusConfigurator AddConsumer<THandler>() where THandler : class
        => AddConsumer(typeof(THandler));

    /// <summary>
    /// Регистрирует тип-хендлер по <see cref="Type"/>. Нужен для статических классов:
    /// они не могут быть аргументом обобщённого метода, а именно так выглядит
    /// рекомендованный стиль метода-хендлера.
    /// </summary>
    /// <remarks>Тип-аргумент в рантайме — reflection-регистрация, legacy-режим (см. <c>AddConsumer&lt;T&gt;</c>).</remarks>
    [RequiresUnreferencedCode(
        "Регистрация хендлера по Type использует рефлексию. Для AOT используйте AddConsumer<T>() с " +
        "подключённым AvtoBus.Generators (тогда тип покрыт сгенерированным диспетчером).")]
    public BusConfigurator AddConsumer(Type handlerType)
    {
        AddConsumerType(handlerType);
        return this;
    }

    [RequiresUnreferencedCode(
        "Рефлексия при регистрации хендлера: разбор интерфейсов/методов. Под AOT тип покрывается генератором, " +
        "и эта ветка недостижима (AvtoBusRegistry.HasGeneratedFor).")]
    private void AddConsumerType(Type type)
    {
        // Source Generator: сгенерированные диспетчеры заменяют reflection для этого типа (док 16).
        if (AvtoBus.Dispatching.AvtoBusRegistry.HasGeneratedFor(type))
        {
            AddGeneratedDispatchers(type);
            return;
        }

        var registered = false;

        // Уровень 1: класс, реализующий IConsumer<T>.
        foreach (var @interface in type.GetInterfaces())
        {
            if (!@interface.IsGenericType)
                continue;

            var definition = @interface.GetGenericTypeDefinition();

            // Вторая линия обороны: IFailedConsumer<T> (идея 169).
            if (definition == typeof(IFailedConsumer<>))
            {
                var failedMessageType = @interface.GetGenericArguments()[0];
                var failedMethod = @interface.GetMethod(nameof(IFailedConsumer<object>.ConsumeAsync))!;

                Services.TryAddConsumerService(type);
                Options.FailedConsumers.Add(FailedHandlerBinder.BindInterface(type, failedMessageType, failedMethod));
                Options.ContractTypes.Add(failedMessageType);
                EnsureConsumerSettings(failedMessageType);
                registered = true;
                continue;
            }

            if (definition != typeof(IConsumer<>))
                continue;

            var messageType = @interface.GetGenericArguments()[0];
            var method = @interface.GetMethod(nameof(IConsumer<object>.ConsumeAsync))!;

            Services.TryAddConsumerService(type);
            Options.Dispatchers.Add(new InterfaceDispatcher(type, messageType, method));
            Options.ContractTypes.Add(messageType);
            EnsureConsumerSettings(messageType);
            registered = true;
        }

        // Уровень 2: методы-хендлеры по конвенции имени.
        foreach (var method in HandlerBinder.FindHandlerMethods(type))
        {
            // Метод интерфейсной реализации уже учтён выше.
            if (registered && method.Name is nameof(IConsumer<object>.ConsumeAsync))
                continue;

            // Метод второй линии обороны: Handle(IFailed<T> failed, ...) (идея 169).
            if (FailedHandlerBinder.IsFailedMethod(method))
            {
                var failedType = method.GetParameters()[0].ParameterType.GetGenericArguments()[0];
                if (!method.IsStatic)
                    Services.TryAddConsumerService(type);

                Options.FailedConsumers.Add(FailedHandlerBinder.BindMethod(method));
                Options.ContractTypes.Add(failedType);
                EnsureConsumerSettings(failedType);
                continue;
            }

            // Батч-хендлер: Handle(IMessageBatch<T> batch, ...) (идея 19).
            if (BatchHandlerBinder.IsBatchMethod(method))
            {
                var batchType = method.GetParameters()[0].ParameterType.GetGenericArguments()[0];
                if (!method.IsStatic)
                    Services.TryAddConsumerService(type);

                Options.BatchDispatchers.Add(BatchHandlerBinder.Bind(method));
                Options.ContractTypes.Add(batchType);
                EnsureConsumerSettings(batchType);
                continue;
            }

            var messageType = HandlerBinder.MessageTypeOf(method);
            if (!IsPlausibleMessageType(messageType))
                continue;

            if (!method.IsStatic)
                Services.TryAddConsumerService(type);

            Options.Dispatchers.Add(HandlerBinder.Bind(method));
            Options.ContractTypes.Add(messageType);
            EnsureConsumerSettings(messageType);

            // Возврат хендлера — тоже контракт: он уйдёт каскадом или ответом,
            // и принимающая сторона должна уметь разрешить его имя.
            foreach (var returned in CascadeTypesOf(method.ReturnType))
                Options.ContractTypes.Add(returned);
        }
    }

    /// <summary>
    /// Подключает сгенерированные диспетчеры: регистрирует тип-хендлер в DI и добавляет
    /// контракты в реестр. Рефлексия для такого типа не запускается вовсе (док 16, §8).
    /// </summary>
    private void AddGeneratedDispatchers(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type handlerType)
    {
        Services.TryAddConsumerService(handlerType);

        foreach (var dispatcher in AvtoBus.Dispatching.AvtoBusRegistry.ForHandlerType(handlerType))
        {
            Options.Dispatchers.Add(dispatcher);
            Options.ContractTypes.Add(dispatcher.MessageType);
            EnsureConsumerSettings(dispatcher.MessageType);
        }
    }

    /// <summary>
    /// Раскладывает тип возврата хендлера на контракты: разворачивает Task/ValueTask,
    /// Result&lt;T&gt; и кортежи.
    /// </summary>
    private static IEnumerable<Type> CascadeTypesOf(Type returnType)
    {
        if (returnType == typeof(void) || returnType == typeof(Task) || returnType == typeof(ValueTask))
            yield break;

        if (returnType.IsGenericType)
        {
            var definition = returnType.GetGenericTypeDefinition();

            if (definition == typeof(Task<>) || definition == typeof(ValueTask<>) || definition == typeof(Result<>))
            {
                foreach (var inner in CascadeTypesOf(returnType.GetGenericArguments()[0]))
                    yield return inner;

                yield break;
            }

            // Кортеж — несколько каскадных сообщений разом.
            if (returnType.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true)
            {
                foreach (var argument in returnType.GetGenericArguments())
                {
                    foreach (var inner in CascadeTypesOf(argument))
                        yield return inner;
                }

                yield break;
            }
        }

        // OutgoingMessages набирается в рантайме — статически типы не известны.
        if (returnType == typeof(OutgoingMessages) || returnType == typeof(Result) || !IsPlausibleMessageType(returnType))
            yield break;

        yield return returnType;
    }

    /// <summary>
    /// Отсекает ложные срабатывания конвенции: метод <c>Handle(string)</c> в случайном классе
    /// не должен превращаться в хендлер.
    /// </summary>
    private static bool IsPlausibleMessageType(Type type)
        => !type.IsPrimitive
           && type != typeof(string)
           && type != typeof(object)
           && type != typeof(decimal)
           && type != typeof(DateTime)
           && type != typeof(Guid)
           && !type.IsEnum;

    /// <summary>
    /// Хендлер-лямбда (Minimal API-стиль): для тестов, прототипов и разовых подписок.
    /// Прод-код — канонически: статический <c>Handle</c> + <c>AddConsumersFromAssembly</c>.
    /// </summary>
    public BusConfigurator Subscribe<T>(Func<T, IServiceProvider, Task> handler) where T : class
    {
        Options.Dispatchers.Add(new DelegateDispatcher(
            typeof(T),
            $"lambda<{typeof(T).Name}>",
            async context => await handler((T)context.Message, context.Services)));

        Options.ContractTypes.Add(typeof(T));
        EnsureConsumerSettings(typeof(T));
        return this;
    }

    public BusConfigurator Subscribe<T>(Func<ConsumeContext<T>, Task> handler) where T : class
    {
        Options.Dispatchers.Add(new DelegateDispatcher(
            typeof(T),
            $"lambda<{typeof(T).Name}>",
            async context => await handler((ConsumeContext<T>)context)));

        Options.ContractTypes.Add(typeof(T));
        EnsureConsumerSettings(typeof(T));
        return this;
    }

    /// <summary>Регистрирует контракт, у которого в этом сервисе нет хендлера (только публикация).</summary>
    public BusConfigurator AddContract<T>() where T : class
    {
        Options.ContractTypes.Add(typeof(T));
        return this;
    }

    /// <summary>
    /// Регистрирует произвольный диспетчер (саги, адаптеры, кастомная обработка).
    /// Тип сообщения попадает в контракты и получает подписку консьюмера (док 17).
    /// </summary>
    public BusConfigurator AddDispatcher(IMessageDispatcher dispatcher)
    {
        Options.Dispatchers.Add(dispatcher);
        Options.ContractTypes.Add(dispatcher.MessageType);
        EnsureConsumerSettings(dispatcher.MessageType);
        return this;
    }

    [RequiresUnreferencedCode(
        "Сканирование сборки на контракты несовместимо с trimming. Под AOT регистрируйте контракты через " +
        "AddContract<T>() — контекст сериализации покрывает их через генератор.")]
    public BusConfigurator AddContractsFromAssembly(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (type.IsAbstract || type.IsInterface || type.IsGenericTypeDefinition)
                continue;

            if (typeof(ICommand).IsAssignableFrom(type) || typeof(IEvent).IsAssignableFrom(type))
                Options.ContractTypes.Add(type);
        }

        return this;
    }
}
