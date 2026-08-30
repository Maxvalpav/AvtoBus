using AvtoBus.Abstractions;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.SchemaRegistry;

public sealed class SchemaRegistryService : BackgroundService
{
    private readonly ISchemaRegistry _registry;
    private readonly ILogger<SchemaRegistryService> _log;
    public SchemaRegistryService(ISchemaRegistry registry, ILogger<SchemaRegistryService> log) { _registry = registry; _log = log; }
    protected override Task ExecuteAsync(CancellationToken ct)
    {
        _log.LogInformation("[SchemaRegistry] registered {Count} schemas", _registry.All.Count);
        return Task.CompletedTask;
    }
}

public static class SchemaRegistryBusExtensions
{
    public static BusConfigurator UseSchemaRegistry(this BusConfigurator bus, Action<ISchemaRegistry> configure)
    {
        var registry = new InMemorySchemaRegistry();
        configure(registry);
        bus.Services.AddSingleton<ISchemaRegistry>(registry);
        bus.Services.AddHostedService<SchemaRegistryService>();
        return bus;
    }

    public static BusConfigurator UseSchemaRegistry(this BusConfigurator bus)
    {
        var registry = new InMemorySchemaRegistry();
        bus.Services.AddSingleton<ISchemaRegistry>(registry);
        bus.Services.AddHostedService<SchemaRegistryService>();
        return bus;
    }
}
