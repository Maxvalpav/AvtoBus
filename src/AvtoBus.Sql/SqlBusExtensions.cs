using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Sql;

public static class SqlBusExtensions
{
    /// <summary>
    /// Подключает SQL-транспорт (идеи 66–67): PostgreSQL таблица-очередь с SKIP LOCKED
    /// и LISTEN/NOTIFY. Первый зарегистрированный транспорт становится транспортом по умолчанию.
    /// </summary>
    public static BusConfigurator UseSql(this BusConfigurator bus, Action<SqlOptions> configure)
    {
        var options = new SqlOptions();
        configure(options);

        bus.Services.AddSingleton(sp => new SqlTransport(options));
        bus.Services.AddSingleton<ITransport>(sp => sp.GetRequiredService<SqlTransport>());
        bus.Services.AddSingleton<AvtoBus.Observability.IConsumerLagProvider>(sp =>
            sp.GetRequiredService<SqlTransport>());

        bus.TrySetDefaultTransport("sql");
        return bus;
    }
}
