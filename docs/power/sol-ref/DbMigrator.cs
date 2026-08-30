using Npgsql;

namespace AvtoBus.Persistence.Postgres;

public static class DbMigrator
{
    public static async Task MigrateAsync(NpgsqlDataSource dataSource, string migrationsRoot, CancellationToken ct = default)
    {
        var dir = new DirectoryInfo(migrationsRoot);
        if (!dir.Exists) return;
        var files = dir.GetFiles("*.sql").OrderBy(f => f.Name).ToArray();
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        foreach (var file in files)
        {
            var sql = await File.ReadAllTextAsync(file.FullName, ct);
            // Каждая миграция — транзакционна (BEGIN/COMMIT внутри sql), но оборачиваем для логирования
            await using var tx = await conn.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = new NpgsqlCommand(sql, conn, tx);
                await cmd.ExecuteNonQueryAsync(ct);
                await tx.CommitAsync(ct);
            }
            catch
            {
                try { await tx.RollbackAsync(ct); } catch { }
                throw;
            }
        }
    }

    public static async Task<int> GetCurrentVersionAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT COALESCE(max(version),0) FROM avtobus.schema_version;", conn);
        var v = await cmd.ExecuteScalarAsync(ct);
        return v is int i ? i : Convert.ToInt32(v ?? 0);
    }
}
