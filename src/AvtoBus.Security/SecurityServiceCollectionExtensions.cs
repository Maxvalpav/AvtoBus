using AvtoBus;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Security;

/// <summary>
/// Точка подключения безопасности к шине (идеи 451, 452, 455, 459):
///
/// services.AddAvtoBusSecurity(bus = bus.UseEnvelopeSecurity(sec =>
/// {
///     // Секрет — из конфигурации (секреты не хранят в коде):
///     // dotnet user-secrets set "AvtoBus:MasterSecret" "..." или env AVTOBUS_MASTERSECRET.
///     sec.MasterSecret = configuration["AvtoBus:MasterSecret"] ?? throw new InvalidOperationException("AvtoBus:MasterSecret is not configured.");
///     sec.RequireSignature = true;
///     sec.EncryptBody = true;
///     sec.OutboundRatePerSecond = 5000;
///     sec.KeyRotationInterval = TimeSpan.FromHours(24);
/// }));
/// </summary>
public static class SecurityServiceCollectionExtensions
{
    /// <summary>Включает подпись/шифрование конвертов и регистрирует фоновую ротацию ключей.</summary>
    public static IServiceCollection AddAvtoBusSecurity(
        this IServiceCollection services,
        Action<SecurityOptions>? configure = null)
    {
        services.TryAddSingleton(TimeProvider.System);

        var options = new SecurityOptions();
        configure?.Invoke(options);
        options.Validate();

        var isDevelopment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development";
        if (options.MasterSecret.Length == 0 && options.Keys.SigningKey.Length == 0)
        {
            if (!isDevelopment && options.RequireSignature)
                throw new InvalidOperationException("SecurityOptions: MasterSecret/Keys must be configured when RequireSignature is enabled outside Development.");
            if (!isDevelopment)
                throw new InvalidOperationException("SecurityOptions: MasterSecret/Keys must be configured outside Development. Set MasterSecret or call UseKeys/UseGeneratedKeys.");
            options.MasterSecret = "avtobus-development-only";
        }

        ProductionSecurityGuard.ThrowIfWeakForProduction(options);

        services.AddSingleton(options);
        services.AddSingleton(sp => new EnvelopeSecurity(
            sp.GetRequiredService<SecurityOptions>(),
            sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IEnvelopeSecurity>(sp => sp.GetRequiredService<EnvelopeSecurity>());
        // Fail-closed principal: раз безопасность подключена — неподписанному
        // avtobus-user больше не доверяем (заменяет HeaderPrincipalExtractor ядра).
        services.Replace(ServiceDescriptor.Singleton<IPrincipalExtractor, SignedPrincipalExtractor>());

        if (options.KeyRotationInterval is not null)
            services.AddHostedService(sp =>
                new SecurityKeyRotationService(
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<EnvelopeSecurity>(),
                    options.KeyRotationInterval.Value,
                    sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<SecurityKeyRotationService>>()));

        return services;
    }

    /// <summary>Подключает уже сконфигурированную безопасность к конфигуратору шины.</summary>
    public static BusConfigurator UseEnvelopeSecurity(
        this BusConfigurator configurator,
        Action<SecurityOptions> configure)
    {
        var options = new SecurityOptions();
        configure(options);
        options.Validate();

        var isDev = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development";
        if (options.MasterSecret.Length == 0 && options.Keys.SigningKey.Length == 0)
        {
            if (!isDev && options.RequireSignature)
                throw new InvalidOperationException("SecurityOptions: MasterSecret/Keys must be configured when RequireSignature is enabled outside Development.");
            if (!isDev)
                throw new InvalidOperationException("SecurityOptions: MasterSecret/Keys must be configured outside Development.");
            options.MasterSecret = "avtobus-development-only";
        }

        ProductionSecurityGuard.ThrowIfWeakForProduction(options);

        var security = new EnvelopeSecurity(options);

        configurator.EnvelopeSecurity = security;
        configurator.Services.AddSingleton(security);
        configurator.Services.AddSingleton<IEnvelopeSecurity>(sp => sp.GetRequiredService<EnvelopeSecurity>());
        // Fail-closed principal (см. выше): неподписанному avtobus-user не доверяем.
        configurator.Services.Replace(ServiceDescriptor.Singleton<IPrincipalExtractor, SignedPrincipalExtractor>());

        if (options.KeyRotationInterval is not null)
        {
            configurator.Services.AddHostedService(sp =>
                new SecurityKeyRotationService(
                    sp.GetRequiredService<TimeProvider>(),
                    security,
                    options.KeyRotationInterval.Value,
                    sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<SecurityKeyRotationService>>()));
        }

        return configurator;
    }
}
