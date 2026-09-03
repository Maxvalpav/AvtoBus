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
public sealed partial class BusConfigurator(IServiceCollection services, BusOptions options)
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
}
