using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Kafka;

public static class KafkaBusExtensions
{
    /// <summary>
    /// Подключает Kafka-транспорт (идеи 57–60). Первый зарегистрированный транспорт
    /// становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseKafka(this BusConfigurator bus, Action<KafkaOptions> configure)
    {
        var options = new KafkaOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new KafkaTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<KafkaTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<KafkaTransport>());

        bus.Options.DefaultTransport = "kafka";
        return bus;
    }
}
