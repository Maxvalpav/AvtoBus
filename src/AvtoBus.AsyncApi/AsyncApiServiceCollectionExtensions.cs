using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.AsyncApi;

public static class AsyncApiServiceCollectionExtensions
{
    /// <summary>Регистрирует генератор AsyncAPI из модели шины (диспетчеры + маршруты).</summary>
    public static IServiceCollection AddAvtoBusAsyncApi(
        this IServiceCollection services,
        Action<AsyncApiInfo>? configure = null)
    {
        // RoutingTable живёт внутри BusOptions; отдаём его в DI, чтобы генератор мог его внедрить.
        services.TryAddSingleton(sp => sp.GetRequiredService<BusOptions>().Routing);

        services.TryAddSingleton(provider =>
        {
            var info = new AsyncApiInfo();
            configure?.Invoke(info);
            return info;
        });

        services.TryAddSingleton<AsyncApiGenerator>();
        return services;
    }
}
