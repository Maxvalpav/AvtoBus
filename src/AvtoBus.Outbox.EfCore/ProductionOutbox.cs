using AvtoBus.Configuration;
using AvtoBus.Security;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Прод-пресет в одном вызове: надёжность ядра + транзакционный outbox +
/// подпись/шифрование конвертов (если задан мастер-секрет).
/// <code>
/// builder.Services.AddAvtoBus(bus => bus
///     .UseInMemory()
///     .UseProductionDefaults&lt;AppDbContext&gt;(o => o.MasterSecret = vault.Secret));
/// </code>
/// Без мастер-секрета безопасность конвертов не включается — только доставка.
/// </summary>
public static class ProductionOutboxExtensions
{
    public static BusConfigurator UseProductionDefaults<TDb>(
        this BusConfigurator bus, Action<ProductionOptions>? configure = null)
        where TDb : DbContext
    {
        var opts = new ProductionOptions();
        configure?.Invoke(opts);

        ProductionDefaultsExtensions.ApplyCore(bus, opts);
        bus.UseOutbox<TDb>();

        if (!string.IsNullOrEmpty(opts.MasterSecret))
        {
            var secret = opts.MasterSecret;
            var rate = opts.OutboundRatePerSecond;
            bus.UseEnvelopeSecurity(sec =>
            {
                sec.MasterSecret = secret;
                sec.RequireSignature = true;
                sec.OutboundRatePerSecond = rate;
            });
        }

        return bus;
    }
}
