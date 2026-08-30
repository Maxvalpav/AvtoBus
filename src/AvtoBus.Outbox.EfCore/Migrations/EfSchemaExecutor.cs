using System.Data.Common;
using AvtoBus.Migrations;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Исполнитель схем для модулей, чьи таблицы мапятся в пользовательский DbContext
/// (outbox, event sourcing, scheduling). Работает через соединение DbContext, поэтому
/// провайдер задаётся пользователем (Npgsql, SQLite, …) — ядро остаётся без БД-зависимостей.
/// Таблица версий — провайдер-нейтральная (TEXT/INT).
/// </summary>
public sealed class EfSchemaExecutor<TDb>(TDb db) : ISchemaExecutor
    where TDb : DbContext
{
    /// <summary>CREATE TABLE IF NOT EXISTS avtobus_schema_versions — идемпотентно, любой провайдер.</summary>
    private const string SchemaTableSql = """
        CREATE TABLE IF NOT EXISTS avtobus_schema_versions (
            module  TEXT PRIMARY KEY,
            version INT NOT NULL
        )
        """;

    public async ValueTask EnsureSchemaTableAsync(CancellationToken ct)
    {
        await db.Database.ExecuteSqlRawAsync(SchemaTableSql, ct).ConfigureAwait(false);
    }

    public async ValueTask ExecuteAsync(string sql, CancellationToken ct)
    {
        await db.Database.ExecuteSqlRawAsync(sql, ct).ConfigureAwait(false);
    }

    public async ValueTask<int> GetVersionAsync(string module, CancellationToken ct)
    {
        await using var command = await CreateCommandAsync(ct).ConfigureAwait(false);
        command.CommandText = "SELECT version FROM avtobus_schema_versions WHERE module = @module";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "@module";
        parameter.Value = module;
        command.Parameters.Add(parameter);

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int version ? version : Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async ValueTask SetVersionAsync(string module, int version, CancellationToken ct)
    {
        // Провайдер-нейтральный upsert: сначала update существующей строки, затем insert, если её не было.
        await using (var command = await CreateCommandAsync(ct).ConfigureAwait(false))
        {
            command.CommandText = "UPDATE avtobus_schema_versions SET version = @version WHERE module = @module";

            var moduleParameter = command.CreateParameter();
            moduleParameter.ParameterName = "@module";
            moduleParameter.Value = module;
            command.Parameters.Add(moduleParameter);

            var versionParameter = command.CreateParameter();
            versionParameter.ParameterName = "@version";
            versionParameter.Value = version;
            command.Parameters.Add(versionParameter);

            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var command = await CreateCommandAsync(ct).ConfigureAwait(false))
        {
            command.CommandText = """
                INSERT INTO avtobus_schema_versions (module, version)
                SELECT @module, @version
                WHERE NOT EXISTS (SELECT 1 FROM avtobus_schema_versions WHERE module = @module)
                """;

            var moduleParameter = command.CreateParameter();
            moduleParameter.ParameterName = "@module";
            moduleParameter.Value = module;
            command.Parameters.Add(moduleParameter);

            var versionParameter = command.CreateParameter();
            versionParameter.ParameterName = "@version";
            versionParameter.Value = version;
            command.Parameters.Add(versionParameter);

            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    private async Task<DbCommand> CreateCommandAsync(CancellationToken ct)
    {
        var connection = db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(ct).ConfigureAwait(false);

        return connection.CreateCommand();
    }
}
