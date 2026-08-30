using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Configuration;

public static class SchemaMigrationExtensions
{
    /// <summary>
    /// Регистрирует версионированную SQL-миграцию модуля (B12). Модуль также обязан
    /// зарегистрировать scoped <see cref="AvtoBus.Migrations.ISchemaExecutor"/> — исполнителя
    /// для своей БД. Если зарегистрирована хотя бы одна миграция, при старте хоста
    /// <see cref="AvtoBus.Migrations.SchemaMigrator"/> применяет неприменённые схемы.
    /// </summary>
    public static BusConfigurator AddSchemaMigration(
        this BusConfigurator bus,
        AvtoBus.Migrations.ISchemaMigration migration)
    {
        bus.Services.AddSingleton(migration);

        // SchemaMigrator регистрируется один раз независимо от числа модулей.
        bus.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, AvtoBus.Migrations.SchemaMigrator>());

        return bus;
    }
}
