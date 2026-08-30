using Npgsql;
using Testcontainers.PostgreSql;

namespace AvtoBus.Tests;

/// <summary>
/// PostgreSQL-инфраструктура для интеграционных тестов (док 15, док 24).
/// Приоритет адреса: env <c>AVTOBUS_PG_URL</c> (CI/локальный сервер), иначе — Testcontainers
/// контейнер <c>postgres:16-alpine</c>. Если Docker недоступен, тесты пропускаются (Assert.Skip).
/// Каждый тест получает отдельную базу данных — полная изоляция без сброса схемы.
/// </summary>
internal static class PostgresTestHost
{
    private const string EnvUrl = "AVTOBUS_PG_URL";

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static PostgreSqlContainer? _container;
    private static bool _unavailable;

    static PostgresTestHost()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
            _container?.DisposeAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Возвращает connection string к свежесозданной базе данных (пустая схема) или null,
    /// если PostgreSQL недоступен. Потокобезопасно: контейнер стартует один раз.
    /// </summary>
    public static async Task<string?> CreateDatabaseAsync()
    {
        var baseCs = await TryGetBaseConnectionStringAsync();
        if (baseCs is null)
            return null;

        var name = "avtobus_test_" + Guid.NewGuid().ToString("N")[..12];

        await using (var admin = new NpgsqlConnection(baseCs))
        {
            await admin.OpenAsync();
            await using var create = admin.CreateCommand();
            create.CommandText = $"CREATE DATABASE \"{name}\"";
            await create.ExecuteNonQueryAsync();
        }

        return new NpgsqlConnectionStringBuilder(baseCs) { Database = name }.ConnectionString;
    }

    private static async Task<string?> TryGetBaseConnectionStringAsync()
    {
        var env = Environment.GetEnvironmentVariable(EnvUrl);
        if (!string.IsNullOrEmpty(env))
            return env;

        if (_unavailable)
            return null;

        await Gate.WaitAsync();
        try
        {
            if (_unavailable)
                return null;

            if (_container is null)
            {
                try
                {
                    var container = new PostgreSqlBuilder("postgres:16-alpine")
                        .WithDatabase("avtobus")
                        .Build();
                    await container.StartAsync();
                    _container = container;
                }
                catch (Exception)
                {
                    _unavailable = true;
                    return null;
                }
            }

            return _container.GetConnectionString();
        }
        finally
        {
            Gate.Release();
        }
    }
}
