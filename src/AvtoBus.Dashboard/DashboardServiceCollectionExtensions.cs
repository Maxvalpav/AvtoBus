using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Dashboard;

/// <summary>DI-регистрация дашборда (док 23): сервис, настройки, аудит.</summary>
public static class DashboardServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует <see cref="DashboardService"/>, <see cref="DashboardOptions"/> и
    /// <see cref="InMemoryDashboardAuditLog"/>. Производственные развороты могут подменить
    /// <see cref="IDashboardAuditLog"/> собственной реализацией.
    /// </summary>
    public static IServiceCollection AddAvtoBusDashboard(
        this IServiceCollection services,
        Action<DashboardOptions>? configure = null)
    {
        services.TryAddSingleton(_ =>
        {
            var options = new DashboardOptions();
            configure?.Invoke(options);
            return options;
        });

        services.TryAddSingleton<IDashboardAuditLog, InMemoryDashboardAuditLog>();
        services.TryAddSingleton<DashboardService>();
        return services;
    }
}
