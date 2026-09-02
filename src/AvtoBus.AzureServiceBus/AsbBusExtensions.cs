using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.AzureServiceBus;

public static class AsbBusExtensions
{
    /// <summary>
    /// Подключает Azure Service Bus-транспорт (идеи 61–62): сессии для строгого порядка,
    /// scheduled enqueue, авто-продление lock-а. Первый зарегистрированный транспорт
    /// становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseAzureServiceBus(this BusConfigurator bus, Action<AsbOptions> configure)
    {
        var options = new AsbOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new AsbTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<AsbTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<AsbTransport>());

        bus.TrySetDefaultTransport("asb");
        return bus;
    }
}
