using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Migrations;

/// <summary>
/// Версионированная SQL-миграция модуля (outbox, event sourcing, scheduling).
/// Каждый модуль поставляет свои схемы: имя модуля + версия + идемпотентный SQL.
/// Применение идёт через <see cref="ISchemaExecutor"/> при старте хоста (B12).
/// </summary>
public interface ISchemaMigration
{
    /// <summary>Имя модуля, например <c>avtobus-outbox</c>. Версии сравниваются в рамках модуля.</summary>
    string ModuleName { get; }

    /// <summary>Монотонная версия схемы: применяется только, если текущая версия модуля ниже.</summary>
    int Version { get; }

    /// <summary>Идемпотентный DDL (CREATE TABLE IF NOT EXISTS). Пустая строка — «no-op» миграция.</summary>
    string Sql { get; }
}

/// <summary>
/// Выполняет SQL и ведёт таблицу версий <c>avtobus_schema_versions</c>.
/// Абстракция живёт в ядре (без зависимостей от конкретной БД), реализации — в модулях:
/// <c>EfSchemaExecutor&lt;TDb&gt;</c> для outbox/ES/scheduling (B12).
/// </summary>
public interface ISchemaExecutor
{
    /// <summary>Гарантирует наличие таблицы версий.</summary>
    ValueTask EnsureSchemaTableAsync(CancellationToken ct);

    /// <summary>Применённая версия модуля: 0, если модуль ещё не мигрирован.</summary>
    ValueTask<int> GetVersionAsync(string module, CancellationToken ct);

    /// <summary>Записывает применённую версию модуля (upsert).</summary>
    ValueTask SetVersionAsync(string module, int version, CancellationToken ct);

    /// <summary>Выполняет один пакет SQL (несколько statements — одним вызовом).</summary>
    ValueTask ExecuteAsync(string sql, CancellationToken ct);
}

/// <summary>
/// Применяет схемы всех зарегистрированных модулей при старте хоста (B12, док 15 §5).
/// Идемпотентно: применяется только то, что ещё не применено (по таблице версий),
/// порядок — по имени модуля, затем по версии. Отката нет: миграции forward-only.
/// </summary>
public sealed class SchemaMigrator(
    IEnumerable<ISchemaMigration> migrations,
    IServiceScopeFactory scopeFactory,
    ILogger<SchemaMigrator> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Исполнитель — scoped (DbContext привязан к скоупу): открываем свой скоуп, а не
        // лезем в корневой контейнер.
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<ISchemaExecutor>();

        await executor.EnsureSchemaTableAsync(cancellationToken).ConfigureAwait(false);

        foreach (var migration in migrations
                     .OrderBy(m => m.ModuleName, StringComparer.Ordinal)
                     .ThenBy(m => m.Version))
        {
            var applied = await executor.GetVersionAsync(migration.ModuleName, cancellationToken).ConfigureAwait(false);

            if (applied >= migration.Version)
            {
                logger.LogDebug("Схема {Module} уже на v{Version}, миграция пропущена",
                    migration.ModuleName, applied);
                continue;
            }

            logger.LogInformation("Применяем схему {Module} v{Version}", migration.ModuleName, migration.Version);

            await executor.ExecuteAsync(migration.Sql, cancellationToken).ConfigureAwait(false);
            await executor.SetVersionAsync(migration.ModuleName, migration.Version, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
