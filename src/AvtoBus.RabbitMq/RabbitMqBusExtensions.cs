using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.RabbitMq;

public static class RabbitMqBusExtensions
{
    /// <summary>
    /// Подключает RabbitMQ-транспорт (идеи 61–62). Первый зарегистрированный транспорт
    /// становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseRabbitMq(this BusConfigurator bus, Action<RabbitMqOptions> configure)
    {
        var options = new RabbitMqOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new RabbitMqTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<RabbitMqTransport>());

        bus.TrySetDefaultTransport("rabbitmq");
        return bus;
    }
}
