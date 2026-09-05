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

        bus.Services.AddSingleton(sp => new RedisTransport(options, sp.GetService<TimeProvider>()));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<RedisTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<RedisTransport>());

        bus.TrySetDefaultTransport("redis");
        return bus;
    }

    /// <summary>
    /// Redis-стор inbox-дедупликации (04 §1.2): атомарный <c>SET NX EX</c> вместо
    /// inbox-таблицы PostgreSQL. Мульти-инстанс без реляционной БД; отказ Redis —
    /// fail-open (at-least-once, хендлеры обязаны быть идемпотентными).
    /// </summary>
    public static BusConfigurator UseRedisInbox(
        this BusConfigurator bus,
        Action<RedisInboxOptions>? configure = null)
    {
        var options = new RedisInboxOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(sp => new RedisInboxStore(
            new RedisOptions { Configuration = options.Configuration },
            options.Window,
            sp.GetService<Microsoft.Extensions.Logging.ILogger<RedisInboxStore>>()));
        bus.Services.AddSingleton<AvtoBus.Runtime.IInboxStore>(sp =>
            sp.GetRequiredService<RedisInboxStore>());
        return bus;
    }
}
