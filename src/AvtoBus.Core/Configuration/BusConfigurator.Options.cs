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

    /// <summary>
    /// Включает дедупликацию входящих сообщений по MessageId (идея 156).
    /// Отрицательное окно запрещено; <c>TimeSpan.Zero</c> — дедуп фактически выключен
    /// (окно нулевое, записи протухают сразу), для отключения просто не вызывайте метод.
    /// </summary>
    public BusConfigurator UseInboxDeduplication(TimeSpan window)
    {
        if (window < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(window), window, "Окно дедупликации не может быть отрицательным.");
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
