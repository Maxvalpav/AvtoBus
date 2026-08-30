using AvtoBus;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Multitenancy;

/// <summary>
/// Точка подключения мультитенантности (идеи 461–467):
///
/// services.AddAvtoBusMultitenancy(tenants => tenants
///     .AddTenant("acme", t => { t.Region = "eu"; t.InboundRatePerSecond = 1000; })
///     .AddTenant("globex", t => { t.Region = "us"; }));
///
/// bus.UseMultitenancy(opt => { opt.CurrentRegion = "eu"; });
/// </summary>
public static class MultitenancyServiceCollectionExtensions
{
    /// <summary>Регистрирует реестр тенантов и data-residency guard в DI.</summary>
    public static IServiceCollection AddAvtoBusMultitenancy(
        this IServiceCollection services,
        Action<TenantOptions>? configure = null)
    {
        services.TryAddSingleton(provider =>
        {
            var options = new TenantOptions();
            configure?.Invoke(options);
            return options;
        });

        services.TryAddSingleton<TenantRegistry>();

        // Data-residency guard подхватывается ядром через BusOptions.RegionPolicy.
        services.TryAddSingleton(sp =>
            new RegionRouteGuard(
                sp.GetRequiredService<TenantRegistry>(),
                sp.GetRequiredService<TenantOptions>()));

        // Изоляция на уровне хранилища (уровни B/C) подхватывается ядром через
        // BusOptions.TenantIsolationPolicy — переписывает destination на исходящем пути
        // и расширяет подписки на per-tenant очереди.
        services.TryAddSingleton(sp =>
            new TenantIsolationPolicy(sp.GetRequiredService<TenantRegistry>()));

        return services;
    }

    /// <summary>
    /// Подключает мультитенантность к конфигуратору шины: регистрирует реестр тенантов,
    /// data-residency guard (проверка на исходящем пути) и per-tenant rate limit
    /// (quota на входящий трафик, идея 464).
    /// </summary>
    public static BusConfigurator UseMultitenancy(
        this BusConfigurator configurator,
        Action<TenantOptions> configure)
    {
        var options = new TenantOptions();
        configure(options);

        configurator.Services.AddSingleton(options);
        configurator.Services.AddSingleton(new TenantRegistry(options));

        var guard = new RegionRouteGuard(
            new TenantRegistry(options),
            options);

        configurator.Options.RegionPolicy = guard;
        configurator.Services.AddSingleton<IRegionPolicy>(guard);

        // Изоляция на уровне хранилища (идея 462, уровни B/C): ядро переписывает destination
        // на исходящем пути и расширяет подписки консьюмеров на per-tenant очереди.
        var isolation = new TenantIsolationPolicy(new TenantRegistry(options));
        configurator.Options.TenantIsolationPolicy = isolation;
        configurator.Services.AddSingleton<ITenantIsolationPolicy>(isolation);

        // Per-tenant квоты на приём: middleware синглтон (держит счётчики окон).
        configurator.Services.AddSingleton(sp =>
        {
            var clock = sp.GetService<TimeProvider>() ?? TimeProvider.System;
            return new TenantRateLimitMiddleware(
                sp.GetRequiredService<TenantRegistry>(),
                clock);
        });

        // Rate limit обязан быть до пользовательских шагов, чтобы квота применялась
        // даже если хендлер не дойдёт до обработки.
        configurator.Pipeline(builder => builder.Use<TenantRateLimitMiddleware>());

        return configurator;
    }
}
