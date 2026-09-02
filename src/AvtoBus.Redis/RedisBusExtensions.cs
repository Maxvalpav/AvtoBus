using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Redis;

public static class RedisBusExtensions
{
    /// <summary>
    /// Подключает Redis Streams-транспорт (идея 65). Первый зарегистрированный транспорт
    /// становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseRedis(this BusConfigurator bus, Action<RedisOptions> configure)
    {
        var options = new RedisOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new RedisTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<RedisTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<RedisTransport>());

        bus.TrySetDefaultTransport("redis");
        return bus;
    }
}
