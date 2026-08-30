using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using AvtoBus.Core;
using AvtoBus.Core.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace AvtoBus.Persistence.Postgres;

public static class AvtoBusServiceCollectionExtensions
{
    public static IServiceCollection AddAvtoBusPostgres(
        this IServiceCollection services,
        Action<AvtoBusOptions> configureBus,
        Action<PostgresAvtoBusOptions>? configurePostgres = null)
    {
        services.AddOptions<AvtoBusOptions>()
            .Configure(configureBus)
            .Validate(x => Uri.TryCreate(x.Source, UriKind.RelativeOrAbsolute, out _),
                "AvtoBus Source must be a URI-reference.")
            .ValidateOnStart();

        services.AddOptions<PostgresAvtoBusOptions>()
            .Configure(options => configurePostgres?.Invoke(options))
            .Validate(options =>
            {
                try { options.Validate(); return true; }
                catch { return false; }
            }, "Invalid PostgreSQL AvtoBus options.")
            .ValidateOnStart();

        services.AddSingleton<ISystemClock, SystemClock>();
        services.AddSingleton<IOutboxSignal, OutboxSignal>();
        services.AddSingleton<IScheduledSignal, ScheduledSignal>();
        services.AddSingleton<AvtoBus.Core.AvtoBusMetrics>();

        services.TryAddSingleton(sp =>
        {
            var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
            return (JsonTypeInfo<Dictionary<string, string>>)
                options.GetTypeInfo(typeof(Dictionary<string, string>));
        });

        services.AddSingleton<PostgresOutboxWriter>();
        services.AddSingleton<PostgresOutboxLeaseStore>();
        services.AddSingleton<PostgresInboxStore>();
        services.AddSingleton<PostgresScheduledWriter>();
        services.AddSingleton<PostgresProcessStore>();
        services.AddSingleton<PostgresDlqStore>();

        services.AddSingleton<IMessageRegistry, MessageRegistry>();
        services.AddSingleton<IConsumerDispatcherRegistry, ConsumerDispatcherRegistry>();
        services.AddSingleton<IMessageBus, MessageBus>();

        // Шифрование payload — опционально, подключается если зарегистрирован IDataEncryptionKeyProvider
        services.TryAddSingleton<AesGcmPayloadProtector>();
        services.TryAddSingleton<IDataEncryptionKeyProvider, NoOpDataEncryptionKeyProvider>();

        // HMAC защита: если IMessageKeyRing не зарегистрирован — используем NoOp (dev) вместо падения DI
        services.TryAddSingleton<IMessageSecurity>(sp =>
        {
            var keyRing = sp.GetService<IMessageKeyRing>();
            return keyRing is null
                ? new NoOpMessageSecurity()
                : new HmacMessageSecurity(keyRing);
        });

        // OTEL meter — экспортируется через AddMeter("AvtoBus") если подключен OpenTelemetry
        services.AddSingleton(sp => sp.GetRequiredService<AvtoBusMetrics>());

        services.AddHostedService<OutboxLeaseDispatcher>();
        services.AddHostedService<ScheduledMessageDispatcher>();
        services.AddHostedService<ConsumerHostedService>();
        services.AddHostedService<RetentionCleanupService>();
        return services;
    }

    public static IServiceCollection AddAvtoBusHealthChecks(this IServiceCollection services)
    {
        services.AddHealthChecks()
            .AddCheck<AvtoBusLivenessHealthCheck>("avtobus_liveness")
            .AddCheck<AvtoBusPostgresReadinessHealthCheck>("avtobus_postgres_readiness")
            .AddCheck<AvtoBusConsumerReadinessHealthCheck>("avtobus_consumer_readiness");
        return services;
    }

    public static IServiceCollection AddAvtoBusDlqApi(this IServiceCollection services)
    {
        services.AddSingleton<DlqService>();
        return services;
    }
}

