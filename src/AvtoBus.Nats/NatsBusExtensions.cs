using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Nats;

public static class NatsBusExtensions
{
    /// <summary>
    /// Подключает NATS/JetStream-транспорт (идеи 63–64). Первый зарегистрированный транспорт
    /// становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseNats(this BusConfigurator bus, Action<NatsOptions> configure)
    {
        var options = new NatsOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new NatsTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<NatsTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<NatsTransport>());

        bus.TrySetDefaultTransport("nats");
        return bus;
    }
}
