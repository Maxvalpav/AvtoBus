using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using AvtoBus.Pipeline;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace AvtoBus;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует шину: <c>services.AddAvtoBus(bus =&gt; bus.UseTransport(...).AddConsumersFromAssembly(...))</c>.
    /// </summary>
    public static IServiceCollection AddAvtoBus(this IServiceCollection services, Action<BusConfigurator> configure)
    {
        var options = new BusOptions();
        // Аварийный readonly по файлу/переменной окружения (идея 497) — срабатывает до пользовательской конфигурации,
        // но пользователь может переопределить через bus.UseReadOnly(false).
        if (Environment.GetEnvironmentVariable("AVTOBUS_READONLY") == "1" ||
            File.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config", "avtobus", "readonly")))
        {
            options.IsReadOnly = true;
            options.ReadOnlyReason = "flag ~/.config/avtobus/readonly or AVTOBUS_READONLY=1";
        }
        var configurator = new BusConfigurator(services, options);
        configure(configurator);

        RegisterCore(services, options);

        services.AddSingleton<ConsumerHost>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ConsumerHost>());
        services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<ConsumerHost>());

        // Канарейка — живой healthcheck всей цепочки (идея 337).
        if (options.CanaryEnabled)
            services.AddHostedService<CanaryProbe>();

        return services;
    }

    /// <summary>
    /// Лёгкий клиент без консьюмеров: только Send/Publish, ноль фоновых сервисов (идея 42).
    /// Для API-гейтвеев, где принимать сообщения не нужно.
    /// </summary>
    public static IServiceCollection AddAvtoBusClient(this IServiceCollection services, Action<BusConfigurator> configure)
    {
        var options = new BusOptions();
        if (Environment.GetEnvironmentVariable("AVTOBUS_READONLY") == "1" ||
            File.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config", "avtobus", "readonly")))
        {
            options.IsReadOnly = true;
            options.ReadOnlyReason = "flag ~/.config/avtobus/readonly or AVTOBUS_READONLY=1";
        }
        var configurator = new BusConfigurator(services, options);
        configure(configurator);

        RegisterCore(services, options);
        return services;
    }

    private static void RegisterCore(IServiceCollection services, BusOptions options)
    {
        services.TryAddSingleton(TimeProvider.System);

        var busOptionsLock = new object();
        services.AddSingleton(sp =>
        {
            lock (busOptionsLock)
            {
                options.EnvelopeSecurity ??= sp.GetService<IEnvelopeSecurity>();
                options.RegionPolicy ??= sp.GetService<IRegionPolicy>();
                options.TenantIsolationPolicy ??= sp.GetService<ITenantIsolationPolicy>();
            }
            return options;
        });

        // Реестр контрактов: всё, что публикуется или обрабатывается этим сервисом.
        services.AddSingleton(_ => MessageRegistry.Build(
            options.ContractTypes.Concat(options.Dispatchers.Select(d => d.MessageType)).Distinct()));

        services.AddSingleton(_ => DispatcherRegistry.Build(options.Dispatchers));

        services.AddSingleton(_ => new FailedConsumerRegistry(options.FailedConsumers));

        services.AddSingleton(sp => new TransportRegistry(
            sp.GetServices<ITransport>(),
            options.DefaultTransport));

        services.AddSingleton(sp => new EnvelopeFactory(
            options,
            sp.GetRequiredService<MessageRegistry>(),
            sp.GetRequiredService<TimeProvider>()));

        services.AddSingleton<ReplyRouter>();

        services.AddSingleton<DlqReader>();

        // Чёрный список на лету (идея 349): паттерны можно добавлять/снимать в рантайме,
        // без рестарта. Включается только если оператор его настроил.
        if (options.BlacklistEnabled)
        {
            services.AddSingleton(sp =>
            {
                var registry = new BlacklistRegistry(logger:
                    sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<BlacklistRegistry>>());
                foreach (var pattern in options.InitialBlacklist)
                    registry.Block(pattern);

                return registry;
            });
            services.AddSingleton<BlacklistMiddleware>(sp => new BlacklistMiddleware(
                sp.GetRequiredService<BlacklistRegistry>(),
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<BlacklistMiddleware>>()));
        }

        services.AddSingleton(sp => new AvtoBusClient(
            sp.GetRequiredService<BusOptions>(),
            sp.GetRequiredService<TransportRegistry>(),
            sp.GetRequiredService<EnvelopeFactory>(),
            sp.GetRequiredService<ReplyRouter>(),
            sp.GetRequiredService<MessageRegistry>(),
            sp.GetService<AvtoBus.Runtime.IUniqueStore>(),
            sp.GetService<AvtoBus.ClaimCheck.ClaimCheckService>(),
            sp.GetService<Microsoft.Extensions.Logging.ILogger<AvtoBus.Runtime.AvtoBusClient>>()));
        services.AddSingleton<IBus>(sp => sp.GetRequiredService<AvtoBusClient>());

        // Scoped-сессия для транзакционной отправки (ADR-0002): сообщения уходят в outbox,
        // если в скоупе зарегистрирован IOutboxSink (пакет AvtoBus.Outbox.EfCore), иначе —
        // немедленно в транспорт. Хендлеры получают её параметром, HTTP-endpoint — из DI.
        services.TryAddScoped<IMessageSession>(sp =>
            new Runtime.MessageSession(sp.GetRequiredService<AvtoBusClient>(), sp));

        // Пайплайн собирается один раз при старте: на горячем пути только вызов делегата.
        // Терминал — вызов хендлеров, поэтому middleware оборачивают обработку целиком:
        // могут замерить её, обернуть в транзакцию или оборвать, не вызвав next.
        services.AddSingleton<BusDelegate>(sp =>
        {
            var builder = new PipelineBuilder();
            foreach (var step in options.PipelineSteps)
                step(builder);

            var dispatchers = sp.GetRequiredService<DispatcherRegistry>();

            // Правильный порядок (outer → inner): Blacklist → Traffic → Debounce → Authorization → Timeout → terminal
            // UseFirst инвертирует, поэтому добавляем в обратном порядке: Timeout, Authorization, Debounce, Traffic, Blacklist
            builder.UseFirst(new TimeoutMiddleware(sp.GetRequiredService<DispatcherRegistry>()));

            builder.UseFirst(new AuthorizationMiddleware(
                sp.GetRequiredService<DispatcherRegistry>(),
                sp.GetRequiredService<IPrincipalExtractor>(),
                sp.GetRequiredService<IAuthorizer>()));

            if (options.Consumers.Values.Any(c => c.DebounceWindow is not null))
                builder.UseFirst(new DebounceMiddleware(options));

            if (options.TrafficAnomalyThreshold > 0)
                builder.UseFirst(new TrafficAnomalyMiddleware(
                    sp.GetRequiredService<TrafficAnomalyDetector>()));

            if (options.BlacklistEnabled)
                builder.UseFirst(sp.GetRequiredService<BlacklistMiddleware>());

            return builder.Build(async context =>
            {
                foreach (var dispatcher in dispatchers.For(context.Message.GetType()))
                    await dispatcher.DispatchAsync(context).ConfigureAwait(false);
            });
        });

        services.AddSingleton<MessageProcessor>();

        // Авторизация хендлеров (идея 453/454): дефолтные реализации можно заменить —
        // Security-модуль подставляет верифицирующий IPrincipalExtractor.
        services.TryAddSingleton<IAuthorizer, DefaultAuthorizer>();
        services.TryAddSingleton<IPrincipalExtractor, HeaderPrincipalExtractor>();

        // Соль PII-маски развёртки применяется один раз при старте (PiiMasker — статический
        // диагностический путь). Пустая соль = встроенный дефолт (корреляция между процессами).
        if (!string.IsNullOrEmpty(options.PiiMaskSalt))
            Diagnostics.PiiMasker.Salt = options.PiiMaskSalt;

        // Аномалия-детектор частоты событий (идея 314).
        if (options.TrafficAnomalyThreshold > 0)
            services.AddSingleton(new TrafficAnomalyDetector(
                options.TrafficAnomalyThreshold,
                options.TrafficAnomalyWindow,
                options.TrafficAnomalyHistory));

        // Единый Meter — версия берётся из сборки (синхрон с BusTelemetry), избегаем дублей
        services.AddSingleton(_ => new System.Diagnostics.Metrics.Meter(BusTelemetry.MeterName, typeof(BusTelemetry).Assembly.GetName().Version?.ToString() ?? "0.1.0"));

        services.AddSingleton(sp =>
        {
            var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
            return meter.CreateObservableGauge<int>(
                "avtobus.queue.depth",
                () => CollectQueueDepths(sp),
                unit: "messages",
                description: "Текущая глубина очереди");
        });

        services.AddSingleton(sp =>
        {
            var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
            return meter.CreateObservableGauge<int>(
                "avtobus.dlq.size",
                () => CollectDlqSizes(sp),
                unit: "messages",
                description: "Количество сообщений, попавших в DLQ");
        });

        services.AddSingleton(sp =>
        {
            var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
            return meter.CreateObservableGauge<long>(
                "avtobus.outbox.pending",
                () => CollectOutboxPending(sp),
                unit: "messages",
                description: "Сообщения в outbox, ожидающие отправки");
        });

        services.AddSingleton(sp =>
        {
            var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
            return meter.CreateObservableGauge<double>(
                "avtobus.outbox.oldest_pending_age",
                () => CollectOutboxOldestAge(sp),
                unit: "s",
                description: "Возраст старейшего неотправленного outbox-сообщения (индикатор застревания)");
        });

        services.AddSingleton(sp =>
        {
            var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
            return meter.CreateObservableGauge<long>(
                "avtobus.consumer.lag",
                () => CollectConsumerLags(sp),
                unit: "messages",
                description: "Необработанные сообщения в источнике подписки консьюмера");
        });
    }

    /// <summary>Собирает глубины очередей всех транспортов-провайдеров в метрики с тегом очереди.</summary>
    private static System.Collections.Generic.IEnumerable<System.Diagnostics.Metrics.Measurement<int>> CollectQueueDepths(
        IServiceProvider provider)
    {
        foreach (var queueDepth in provider.GetServices<IQueueDepthProvider>())
        {
            foreach (var kvp in queueDepth.QueueDepths)
                yield return new System.Diagnostics.Metrics.Measurement<int>(
                    kvp.Value,
                    new System.Collections.Generic.KeyValuePair<string, object?>("avtobus.queue.name", kvp.Key));
        }
    }

    /// <summary>Собирает глубины только DLQ-очередей. Суффикс задаётся транспортом через конвенцию имени.</summary>
    private static System.Collections.Generic.IEnumerable<System.Diagnostics.Metrics.Measurement<int>> CollectDlqSizes(
        IServiceProvider provider)
    {
        foreach (var queueDepth in provider.GetServices<IQueueDepthProvider>())
        {
            foreach (var kvp in queueDepth.QueueDepths)
            {
                if (!IsDlqName(kvp.Key))
                    continue;

                yield return new System.Diagnostics.Metrics.Measurement<int>(
                    kvp.Value,
                    new System.Collections.Generic.KeyValuePair<string, object?>("avtobus.queue.name", kvp.Key));
            }
        }
    }

    /// <summary>Собирает число ожидающих отправки outbox-сообщений всех провайдеров.</summary>
    private static System.Collections.Generic.IEnumerable<System.Diagnostics.Metrics.Measurement<long>> CollectOutboxPending(
        IServiceProvider provider)
    {
        foreach (var outbox in provider.GetServices<IOutboxPendingProvider>())
            yield return new System.Diagnostics.Metrics.Measurement<long>(outbox.OutboxPending);
    }

    /// <summary>Собирает возраст старейшего ожидающего outbox-сообщения (секунды).</summary>
    private static System.Collections.Generic.IEnumerable<System.Diagnostics.Metrics.Measurement<double>> CollectOutboxOldestAge(
        IServiceProvider provider)
    {
        var now = DateTime.UtcNow;
        foreach (var outbox in provider.GetServices<IOutboxHealthProvider>())
        {
            if (outbox.OldestPendingAt is { } oldest)
                yield return new System.Diagnostics.Metrics.Measurement<double>((now - oldest).TotalSeconds);
        }
    }

    /// <summary>Собирает отставания всех консьюмеров с тегом имени destination.</summary>
    private static System.Collections.Generic.IEnumerable<System.Diagnostics.Metrics.Measurement<long>> CollectConsumerLags(
        IServiceProvider provider)
    {
        foreach (var lag in provider.GetServices<IConsumerLagProvider>())
        {
            foreach (var kvp in lag.ConsumerLags)
                yield return new System.Diagnostics.Metrics.Measurement<long>(
                    kvp.Value,
                    new System.Collections.Generic.KeyValuePair<string, object?>("avtobus.destination.name", kvp.Key));
        }
    }

    /// <summary>DLQ-очередь по конвенции имени транспорта: <c>*.error</c>, <c>*.poison</c>, <c>*.expired</c>.</summary>
    private static bool IsDlqName(string queueName)
        => queueName.EndsWith(".error", StringComparison.Ordinal)
           || queueName.EndsWith(".poison", StringComparison.Ordinal)
           || queueName.EndsWith(".expired", StringComparison.Ordinal);
}

public static class AvtoBusHealthCheckExtensions
{
    /// <summary>Регистрирует AvtoBusHealthCheck как HealthCheck (идея 35, N-1 деплой).</summary>
    public static IServiceCollection AddAvtoBusHealthCheck(this IServiceCollection services, long lagThreshold = 10_000, string name = "avtobus")
    {
        services.AddSingleton(sp => new AvtoBus.Observability.AvtoBusHealthCheck(
            sp.GetRequiredService<AvtoBus.Runtime.ConsumerHost>(), lagThreshold));
        services.AddHealthChecks().AddCheck<AvtoBus.Observability.AvtoBusHealthCheck>(name);
        return services;
    }
}
