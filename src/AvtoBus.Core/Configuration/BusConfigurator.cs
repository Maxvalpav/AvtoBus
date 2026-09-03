using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization;
using AvtoBus.Handlers;
using AvtoBus.Pipeline;
using AvtoBus.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Configuration;

/// <summary>
/// Всё, что настроено при старте и заморожено на время жизни приложения.
/// </summary>
public sealed class BusOptions
{
    public RoutingTable Routing { get; } = new();

    public RecoverabilitySettings Recoverability { get; } = new();

    public SerializerRegistry Serializers { get; } = CreateDefaultSerializerRegistry();

    /// <summary>
    /// Source-generated <see cref="JsonSerializerContext"/>, зарегистрированный через
    /// <c>UseJsonSerializerContext</c>: сериализация контрактов идёт через <see cref="JsonTypeInfo"/>
    /// без рефлексии (AOT). <c>null</c> — только reflection-режим.
    /// </summary>
    public JsonSerializerContext? JsonContext { get; set; }

    public Dictionary<Type, ConsumerSettings> Consumers { get; } = [];

    /// <summary>Имя транспорта по умолчанию.</summary>
    public string DefaultTransport { get; set; } = "inmemory";

    /// <summary>Группа консьюмеров по умолчанию — обычно имя сервиса.</summary>
    public string ServiceName { get; set; } = "avtobus";

    /// <summary>Дедупликация входящих по MessageId (идея 156).</summary>
    public TimeSpan? InboxWindow { get; set; }

    /// <summary>Порог circuit breaker: сколько ошибок подряд размыкают цепь (идея 163).</summary>
    public int CircuitBreakerThreshold { get; set; }

    public TimeSpan CircuitBreakerDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Таймаут ожидания ответа в request/response.</summary>
    public TimeSpan DefaultRequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Канарейка: системное сообщение через всю цепочку — живой healthcheck (идея 337).</summary>
    public bool CanaryEnabled { get; set; }

    /// <summary>Подключённая подсистема безопасности конвертов (подпись/шифрование, идея 451).</summary>
    public IEnvelopeSecurity? EnvelopeSecurity { get; set; }

    /// <summary>
    /// Политика data-residency (идея 467): запрещает маршруты между регионами, нарушающие
    /// принадлежность данных. Реализуется в <c>AvtoBus.Multitenancy</c>.
    /// </summary>
    public IRegionPolicy? RegionPolicy { get; set; }

    /// <summary>
    /// Политика изоляции тенантов на уровне хранилища (идея 462, уровни B/C):
    /// переписывает destination на исходящем пути и расширяет подписки на входящем.
    /// Реализуется в <c>AvtoBus.Multitenancy</c>.
    /// </summary>
    public ITenantIsolationPolicy? TenantIsolationPolicy { get; set; }

    /// <summary>
    /// Маскировать PII-поля контрактов (<see cref="AvtoBus.Contracts.PersonalDataAttribute"/>) в диагностике
    /// второй линии обороны и DLQ-описаниях.
    /// </summary>
    public bool PiiMaskingEnabled { get; set; }

    /// <summary>
    /// Соль детерминированной PII-маски (pepper развёртки). Пусто — встроенный дефолт
    /// (маски коррелируются между процессами). Задай свой секрет, чтобы утечка логов
    /// не давала брутфорсить короткие PII перебором входов.
    /// </summary>
    public string PiiMaskSalt { get; set; } = "";

    public TimeSpan CanaryInterval { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan CanaryTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Аномалия-детектор частоты событий (идея 314): во сколько раз рост/падение — аномалия.</summary>
    public double TrafficAnomalyThreshold { get; set; }

    public TimeSpan TrafficAnomalyWindow { get; set; } = TimeSpan.FromMinutes(1);

    public int TrafficAnomalyHistory { get; set; } = 12;

    /// <summary>Лимиты контекста: максимальный объём заголовков конверта и число хопов (идея 313).</summary>
    public int MaxHeaderBytes { get; set; } = 16 * 1024;

    public int MaxHeaderCount { get; set; } = 64;

    public int MaxHops { get; set; } = 50;

    /// <summary>Чёрный список на лету: паттерны, заблокированные оператором во время работы (идея 349).</summary>
    public bool BlacklistEnabled { get; set; }

    public IReadOnlyCollection<string> InitialBlacklist { get; set; } = Array.Empty<string>();

    /// <summary>ClaimCheck: тела крупнее порога уходят в blob-store (идея 138).</summary>
    public AvtoBus.ClaimCheck.ClaimCheckOptions? ClaimCheck { get; set; }

    /// <summary>Сжатие тел сообщений (gzip).</summary>
    public AvtoBus.Compression.CompressionOptions? Compression { get; set; }

    /// <summary>Allowlist типов — если задан, только они проходят десериализацию (идея 451).</summary>
    public HashSet<string>? AllowedMessageTypes { get; set; }

    /// <summary>mTLS опции, пробрасываются транспортам.</summary>
    public object? TlsOptions { get; set; }

    /// <summary>Профиль данных: Gdpr/152-ФЗ включает маскирование PII по умолчанию (идея 498).</summary>
    public DataProfile DataProfile { get; set; } = DataProfile.Default;

    /// <summary>Аварийный режим «только чтение»: исходящие публикации блокируются (идея 497).</summary>
    public bool IsReadOnly { get; set; }

    public string ReadOnlyReason { get; set; } = "readonly by operator";

    /// <summary>Локальные in-process очереди (идея 15).</summary>
    public Dictionary<string, int> LocalQueues { get; } = [];

    /// <summary>Ждать ли завершения in-flight обработок при остановке (идея 35).</summary>
    public TimeSpan ShutdownDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    internal List<Action<PipelineBuilder>> PipelineSteps { get; } = [];

    internal List<IMessageDispatcher> Dispatchers { get; } = [];

    /// <summary>Типы middleware, добавленные в пайплайн ровно один раз (саги, outbox).</summary>
    internal HashSet<Type> UniqueMiddlewareTypes { get; } = [];

    /// <summary>Хендлеры второй линии обороны: принимают сообщения, исчерпавшие ретраи (идея 169).</summary>
    internal List<IFailedConsumerDispatcher> FailedConsumers { get; } = [];

    /// <summary>Батч-хендлеры: одна обработка на N сообщений (идея 19).</summary>
    internal List<IBatchDispatcher> BatchDispatchers { get; } = [];

    internal HashSet<Type> ContractTypes { get; } = [];

    /// <summary>
    /// Дефолтный сериализатор — reflection-режим STJ (legacy). Под AOT его заменяет
    /// <c>UseJsonSerializerContext</c>: suppression оправдана, так как reflection-инстанс
    /// конструируется только когда пользователь не задал source-generated контекст.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Дефолт — reflection-режим (док 01 §codegen). AOT-приложения вызывают UseJsonSerializerContext, " +
        "который пересоздаёт сериализатор с контекстом; reflection-ветка под trimming не используется.")]
    private static SerializerRegistry CreateDefaultSerializerRegistry()
        => new(new JsonMessageSerializer());
}

public enum DataProfile
{
    Default = 0,
    Gdpr = 1,
    Ru152Fz = 2
}

/// <summary>
/// Точка настройки шины: <c>services.AddAvtoBus(bus =&gt; ...)</c>.
/// </summary>
public sealed class BusConfigurator(IServiceCollection services, BusOptions options)
{
    public IServiceCollection Services { get; } = services;

    public BusOptions Options { get; } = options;

    /// <summary>
    /// Подключённая подсистема безопасности конвертов (подпись/шифрование, идея 451).
    /// Проксируется на <see cref="BusOptions.EnvelopeSecurity"/>, чтобы ядро (EnvelopeFactory,
    /// MessageProcessor) видело его без отдельной регистрации.
    /// </summary>
    public IEnvelopeSecurity? EnvelopeSecurity
    {
        get => Options.EnvelopeSecurity;
        set => Options.EnvelopeSecurity = value;
    }

    /// <summary>
    /// Маскировать PII-поля контрактов (<see cref="AvtoBus.Contracts.PersonalDataAttribute"/>) в диагностике
    /// второй линии обороны и DLQ-описаниях (идея 456).
    /// </summary>
    public bool PiiMaskingEnabled
    {
        get => Options.PiiMaskingEnabled;
        set => Options.PiiMaskingEnabled = value;
    }

    /// <summary>Соль PII-маски развёртки (см. <see cref="BusOptions.PiiMaskSalt"/>).</summary>
    public string PiiMaskSalt
    {
        get => Options.PiiMaskSalt;
        set => Options.PiiMaskSalt = value;
    }

    /// <summary>Имя сервиса: используется как группа консьюмеров по умолчанию.</summary>
    public BusConfigurator ServiceName(string name)
    {
        Options.ServiceName = name;
        return this;
    }

    public bool IsDefaultTransportSet => _defaultTransportSet;
    public void TrySetDefaultTransport(string name)
    {
        if (!_defaultTransportSet) { Options.DefaultTransport = name; _defaultTransportSet = true; }
    }
    private bool _defaultTransportSet;

    /// <summary>
    /// Регистрирует транспорт. Первый зарегистрированный становится транспортом по умолчанию.
    /// </summary>
    public BusConfigurator UseTransport(ITransport transport)
    {
        Services.AddSingleton(transport);
        TrySetDefaultTransport(transport.Name);
        return this;
    }

    /// <summary>Регистрирует транспорт, которому нужны зависимости из контейнера.</summary>
    public BusConfigurator UseTransport(string name, Func<IServiceProvider, ITransport> factory)
    {
        Services.AddSingleton<ITransport>(factory);
        TrySetDefaultTransport(name);
        return this;
    }

    // ---- Хендлеры -------------------------------------------------------

    /// <summary>
    /// Находит в сборке всё, что похоже на хендлер: реализации <see cref="IConsumer{T}"/>
    /// и статические/инстансные методы Handle/Consume (идея 1).
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

    /// <summary>Уровень 3: хендлер-лямбда (Minimal API-стиль).</summary>
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

    // ---- Настройка ------------------------------------------------------

    /// <summary>Fluent-настройка конкретного консьюмера.</summary>
    public ConsumerConfigurator<T> Consumer<T>() where T : class
        => new(EnsureConsumerSettings(typeof(T)));

    public BusConfigurator Routes(Action<RouteConfigurator> configure)
    {
        configure(new RouteConfigurator(Options.Routing));
        return this;
    }

    public BusConfigurator Recoverability(Action<RecoverabilitySettings> configure)
    {
        configure(Options.Recoverability);
        return this;
    }

    /// <summary>Добавляет пользовательские шаги в пайплайн обработки.</summary>
    public BusConfigurator Pipeline(Action<PipelineBuilder> configure)
    {
        Options.PipelineSteps.Add(configure);
        return this;
    }

    public BusConfigurator Serialization(Action<SerializerRegistry> configure)
    {
        configure(Options.Serializers);
        return this;
    }

    /// <summary>
    /// Регистрирует source-generated <see cref="JsonSerializerContext"/>: сериализация переходит
    /// на AOT-safe путь через <see cref="JsonTypeInfo"/>. Контекст обязан покрывать все контракты шины —
    /// его генерирует AvtoBus.Generators для типов-хендлеров (док 16, §6).
    /// </summary>
    public BusConfigurator UseJsonSerializerContext(JsonSerializerContext context)
    {
        Options.JsonContext = context;
        Options.Serializers.SetDefault(NewJsonSerializer(context));
        return this;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Контекст переводит сериализатор на AOT-safe путь (JsonTypeInfo); класс JsonMessageSerializer " +
        "помечен RUC только из-за его reflection-ветки для непокрытых типов.")]
    private static JsonMessageSerializer NewJsonSerializer(JsonSerializerContext context)
        => new(context);

    /// <summary>Включает дедупликацию входящих сообщений по MessageId (идея 156).</summary>
    public BusConfigurator UseInboxDeduplication(TimeSpan window)
    {
        Options.InboxWindow = window;
        return this;
    }

    /// <summary>Размыкает цепь консьюмера после N ошибок подряд (идея 163).</summary>
    public BusConfigurator UseCircuitBreaker(int threshold, TimeSpan? duration = null)
    {
        Options.CircuitBreakerThreshold = threshold;
        if (duration is { } value)
            Options.CircuitBreakerDuration = value;
        return this;
    }

    /// <summary>
    /// Канарейка (идея 337): системное сообщение проходит publish → транспорт → consume каждые
    /// <paramref name="interval"/> — его время полного цикла живой end-to-end healthcheck.
    /// </summary>
    public BusConfigurator UseCanary(TimeSpan? interval = null, TimeSpan? timeout = null)
    {
        Options.CanaryEnabled = true;
        if (interval is { } i)
            Options.CanaryInterval = i;
        if (timeout is { } t)
            Options.CanaryTimeout = t;
        return this;
    }

    /// <summary>
    /// Аномалия-детектор частоты сообщений (идея 314): завершённое окно vs среднее предыдущих —
    /// рост/падение в <paramref name="threshold"/> раз порождает <see cref="TrafficAnomalyDetector.Anomalies"/>.
    /// </summary>
    public BusConfigurator UseTrafficAnomalyDetection(double threshold = 10, TimeSpan? window = null, int history = 12)
    {
        Options.TrafficAnomalyThreshold = threshold;
        Options.TrafficAnomalyWindow = window ?? TimeSpan.FromMinutes(1);
        Options.TrafficAnomalyHistory = history;
        return this;
    }

    /// <summary>
    /// Лимиты контекста сообщения (идея 313): максимальный объём заголовков и число хопов,
    /// после которых наследуемые header-ы обрезаются (защита от «раздувания» через длинную цепочку).
    /// </summary>
    public BusConfigurator UseHeaderLimits(int maxBytes = 16 * 1024, int maxCount = 64, int maxHops = 50)
    {
        Options.MaxHeaderBytes = maxBytes;
        Options.MaxHeaderCount = maxCount;
        Options.MaxHops = maxHops;
        return this;
    }

    /// <summary>
    /// Чёрный список на лету (идея 349): консьюмеры дропают заблокированные типы/паттерны.
    /// Пополняется через <see cref="BlacklistRegistry.Block"/> без рестарта. <paramref name="initial"/>
    /// — паттерны, заблокированные уже при старте (например, контракт на удалении).
    /// </summary>
    public BusConfigurator UseBlacklist(params string[] initial)
    {
        Options.BlacklistEnabled = true;
        Options.InitialBlacklist = initial;
        return this;
    }

    /// <summary>Включает сжатие тел &gt; threshold (gzip, идея 105).</summary>
    public BusConfigurator UseCompression(int thresholdBytes = 1024, System.IO.Compression.CompressionLevel level = System.IO.Compression.CompressionLevel.Optimal)
    {
        if (thresholdBytes < 1) throw new ArgumentOutOfRangeException(nameof(thresholdBytes));
        if (Options.Compression is not null) return this;
        var opts = new AvtoBus.Compression.CompressionOptions { ThresholdBytes = thresholdBytes, Level = level };
        Options.Compression = opts;
        Services.AddSingleton(opts);
        Services.AddSingleton<AvtoBus.Pipeline.CompressionMiddleware>();
        Options.PipelineSteps.Add(b => b.Use<AvtoBus.Pipeline.CompressionMiddleware>());
        return this;
    }

    /// <summary>Профиль данных (идея 498): Gdpr/152-ФЗ включает PII-маскирование по умолчанию.</summary>
    public BusConfigurator UseDataProfile(DataProfile profile)
    {
        Options.DataProfile = profile;
        if (profile is DataProfile.Gdpr or DataProfile.Ru152Fz)
            Options.PiiMaskingEnabled = true;
        return this;
    }

    /// <summary>Аварийный режим «только чтение» (идея 497): блокирует исходящие каскады, обработка остаётся.</summary>
    public BusConfigurator UseReadOnly(bool enabled = true, string reason = "readonly by operator")
    {
        Options.IsReadOnly = enabled;
        Options.ReadOnlyReason = reason;
        return this;
    }

    /// <summary>Ограничивает десериализацию только allowlist-типами (fail-closed).</summary>
    public BusConfigurator UseAllowlist(params string[] messageTypes)
    {
        Options.AllowedMessageTypes ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var t in messageTypes) Options.AllowedMessageTypes.Add(t);
        return this;
    }
    public BusConfigurator UseAllowlist(IEnumerable<string> messageTypes)
    {
        Options.AllowedMessageTypes ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var t in messageTypes) Options.AllowedMessageTypes.Add(t);
        return this;
    }

    /// <summary>Включает ClaimCheck: тела крупнее порога уходят в blob-store (идея 138).</summary>
    public BusConfigurator UseClaimCheck(int thresholdBytes = 256 * 1024, AvtoBus.ClaimCheck.IBlobStore? store = null)
    {
        if (Options.ClaimCheck is not null) return this;
        Options.ClaimCheck = new AvtoBus.ClaimCheck.ClaimCheckOptions { ThresholdBytes = thresholdBytes };
        if (store is not null)
            Services.AddSingleton(store);
        else
            Services.TryAddSingleton<AvtoBus.ClaimCheck.IBlobStore, AvtoBus.ClaimCheck.InMemoryBlobStore>();
        Services.AddSingleton<AvtoBus.ClaimCheck.ClaimCheckService>();
        return this;
    }

    /// <summary>Регистрирует локальную in-process очередь (идея 15).</summary>
    public BusConfigurator AddLocalQueue(string name, int capacity = 10_000)
    {
        Options.LocalQueues[name] = capacity;
        Services.TryAddSingleton<AvtoBus.Local.LocalQueueTransport>(sp =>
            new AvtoBus.Local.LocalQueueTransport(
                Options.LocalQueues.Select(kv => new AvtoBus.Local.LocalQueueSettings(kv.Key, kv.Value)),
                sp.GetService<TimeProvider>()));
        Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<AvtoBus.Local.LocalQueueTransport>());
        return this;
    }

    private ConsumerSettings EnsureConsumerSettings(Type messageType)
    {
        if (Options.Consumers.TryGetValue(messageType, out var existing))
            return existing;

        var settings = new ConsumerSettings { MessageType = messageType };
        Options.Consumers[messageType] = settings;
        return settings;
    }
}

internal static class ServiceCollectionHandlerExtensions
{
    /// <summary>
    /// Регистрирует тип-хендлер как scoped, не дублируя уже существующую регистрацию.
    /// Статические классы не регистрируются: их методы вызываются напрямую без DI (док 16 §3),
    /// а AddScoped(static) падает при построении контейнера.
    /// </summary>
    public static void TryAddConsumerService(
        this IServiceCollection services,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type type)
    {
        if (type.IsAbstract && type.IsSealed)
            return;

        if (services.Any(descriptor => descriptor.ServiceType == type))
            return;

        services.AddScoped(type);
    }
}

/// <summary>Диспетчер для классов, реализующих <see cref="IConsumer{T}"/>.</summary>
internal sealed class InterfaceDispatcher : IMessageDispatcher, IHandlerTimeoutProvider, IHandlerAuthorizationProvider
{
    private readonly Type _handlerType;
    private readonly Func<object, ConsumeContext, Task> _invoke;
    private readonly MethodInfo _method;

    [RequiresUnreferencedCode(
        "Компиляция вызова через Expression — reflection-путь (legacy). Под AOT этот диспетчер " +
        "не создаётся: IConsumer<T> покрывается генератором.")]
    public InterfaceDispatcher(Type handlerType, Type messageType, MethodInfo method)
    {
        _handlerType = handlerType;
        MessageType = messageType;
        HandlerName = $"{handlerType.Name}.{method.Name}";
        _method = method;

        var contextType = typeof(ConsumeContext<>).MakeGenericType(messageType);
        var handler = System.Linq.Expressions.Expression.Parameter(typeof(object), "handler");
        var context = System.Linq.Expressions.Expression.Parameter(typeof(ConsumeContext), "ctx");

        var call = System.Linq.Expressions.Expression.Call(
            System.Linq.Expressions.Expression.Convert(handler, handlerType),
            method,
            System.Linq.Expressions.Expression.Convert(context, contextType));

        _invoke = System.Linq.Expressions.Expression
            .Lambda<Func<object, ConsumeContext, Task>>(call, handler, context)
            .Compile();
    }

    public Type MessageType { get; }

    public string HandlerName { get; }

    public TimeSpan? Timeout
        => _method.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout
           ?? _handlerType.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout;

    public BusAuthorizeAttribute? Authorization
        => _method.GetCustomAttribute<BusAuthorizeAttribute>()
           ?? _handlerType.GetCustomAttribute<BusAuthorizeAttribute>();

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var handler = context.Services.GetRequiredService(_handlerType);
        await _invoke(handler, context).ConfigureAwait(false);
    }
}
