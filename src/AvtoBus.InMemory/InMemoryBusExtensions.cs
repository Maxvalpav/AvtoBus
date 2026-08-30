using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.InMemory;

public static class InMemoryBusExtensions
{
    /// <summary>
    /// Включает in-memory транспорт. Подходит и для тестов, и для модульного монолита:
    /// при выносе модуля в сервис меняется только эта строка (идея 27).
    /// </summary>
    public static BusConfigurator UseInMemory(this BusConfigurator bus, int capacity = 10_000)
    {
        bus.Services.AddSingleton(sp => new InMemoryTransport(sp.GetRequiredService<TimeProvider>(), capacity));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<InMemoryTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IQueueDepthProvider>(sp =>
            sp.GetRequiredService<InMemoryTransport>());
        bus.Options.DefaultTransport = "inmemory";
        return bus;
    }
}
