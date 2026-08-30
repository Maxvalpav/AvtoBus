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
///     sec.MasterSecret = "shared-secret";
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

        services.AddSingleton(provider =>
        {
            var options = new SecurityOptions();
            configure?.Invoke(options);

            if (options.MasterSecret.Length == 0 && options.Keys.SigningKey.Length == 0)
            {
                // В разработке удобно, чтобы работало «из коробки» (детерминированный тестовый ключ).
                options.MasterSecret = "avtobus-development-only";
            }

            var security = new EnvelopeSecurity(options);

            // Ротация по расписанию менять ключи в рантайме (идея 452).
            if (options.KeyRotationInterval is not null)
                services.AddHostedService(sp =>
                    new SecurityKeyRotationService(
                        sp.GetRequiredService<TimeProvider>(),
                        security,
                        options.KeyRotationInterval.Value,
                        sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<SecurityKeyRotationService>>()));

            return security;
        });

        return services;
    }

    /// <summary>Подключает уже сконфигурированную безопасность к конфигуратору шины.</summary>
    public static BusConfigurator UseEnvelopeSecurity(
        this BusConfigurator configurator,
        Action<SecurityOptions> configure)
    {
        var options = new SecurityOptions();
        configure(options);

        var security = new EnvelopeSecurity(options);

        configurator.EnvelopeSecurity = security;
        configurator.Services.AddSingleton(security);

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
