using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using AvtoBus.Configuration;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Регистрация: одна строка <c>bus.UseOutbox&lt;AppDbContext&gt;()</c> включает transactional outbox,
/// inbox-дедупликацию и авточистку (док 15, §8). Interceptor подключается пользователем к DbContext:
/// <c>opt.UseNpgsql(cs).AddInterceptors(sp.GetRequiredService&lt;OutboxSaveChangesInterceptor&gt;())</c>.
/// </summary>
public static class OutboxRegistration
{
    public static BusConfigurator UseOutbox<TDb>(
        this BusConfigurator bus, Action<OutboxOptions>? configure = null)
        where TDb : DbContext
    {
        var opt = new OutboxOptions();
        configure?.Invoke(opt);
        opt.Validate();

        bus.Services.AddSingleton(opt);
        bus.Services.TryAddSingleton(TimeProvider.System);
        bus.Services.AddSingleton<IOutboxSignal, ChannelOutboxSignal>();
        bus.Services.AddSingleton<OutboxSaveChangesInterceptor>();
        bus.Services.AddSingleton<IEnvelopeSerializer, JsonEnvelopeSerializer>();
        // Relay/cleanup резолвят базовый DbContext: маппим его на TDb, иначе фоновые
        // задачи падают с InvalidOperationException, хост отменяет старт и SchemaMigrator
        // получает отменённый токен (загадочный OperationCanceledException вместо причины).
        // TryAdd — явный маппинг пользователя побеждает.
        bus.Services.TryAddScoped<DbContext>(sp => sp.GetRequiredService<TDb>());
        // Один инстанс на скоуп: и прямой доступ (IOutbox), и синк сессии (IOutboxSink) — один объект.
        bus.Services.AddScoped<EfCoreOutbox<TDb>>();
        bus.Services.AddScoped<IOutbox>(sp => sp.GetRequiredService<EfCoreOutbox<TDb>>());
        bus.Services.AddScoped<IOutboxSink>(sp => sp.GetRequiredService<EfCoreOutbox<TDb>>());
        bus.Services.AddHostedService<OutboxRelay>();
        bus.Services.AddSingleton<AvtoBus.Observability.IOutboxPendingProvider>(sp =>
            sp.GetRequiredService<OutboxRelay>());
        bus.Services.AddHostedService<OutboxCleanup>();
        // B12: схема outbox/inbox + таблица версий
        bus.Services.AddSingleton<AvtoBus.Migrations.ISchemaMigration, AvtoBus.Outbox.EfCore.OutboxSchemaMigration>();
        bus.Services.AddSingleton<AvtoBus.Migrations.ISchemaMigration, AvtoBus.Outbox.EfCore.OutboxSchemaMigrationV2>();
        bus.Services.AddSingleton<AvtoBus.Migrations.ISchemaMigration, AvtoBus.Outbox.EfCore.OutboxSchemaMigrationV3>();
        bus.Services.AddScoped<AvtoBus.Migrations.ISchemaExecutor>(sp => new AvtoBus.Outbox.EfCore.EfSchemaExecutor<TDb>(sp.GetRequiredService<TDb>()));
        bus.Services.AddHostedService<AvtoBus.Migrations.SchemaMigrator>();
        return bus;
    }
}
