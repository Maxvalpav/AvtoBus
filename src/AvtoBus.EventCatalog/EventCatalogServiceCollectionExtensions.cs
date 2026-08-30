using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.EventCatalog;

public static class EventCatalogServiceCollectionExtensions
{
    /// <summary>Регистрирует генератор Event Catalog из модели шины.</summary>
    public static IServiceCollection AddAvtoBusEventCatalog(
        this IServiceCollection services,
        Action<EventCatalogOptions>? configure = null)
    {
        services.TryAddSingleton(provider =>
        {
            var options = new EventCatalogOptions();
            configure?.Invoke(options);
            return options;
        });

        services.TryAddSingleton<EventCatalogGenerator>();
        return services;
    }
}
