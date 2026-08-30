using AvtoBus.Sql;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для SQL-транспорта (док 18 §7, идея 98). Требует реального
/// PostgreSQL: тесты пропускаются, если переменная окружения AVTOBUS_PG_URL не задана.
/// В CI задаётся адрес инфраструктуры (напр. Host=postgres;Database=avtobus;...).
/// </summary>
public sealed class SqlTransportConformanceTests : TransportConformanceTests
{
    private const string EnvUrl = "AVTOBUS_PG_URL";

    public SqlTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvUrl)))
            Assert.Skip("PostgreSQL недоступен: задайте AVTOBUS_PG_URL");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var url = Environment.GetEnvironmentVariable(EnvUrl)!;
        var options = new SqlOptions
        {
            ConnectionString = url,
            Name = $"avtobus-conformance-{Guid.NewGuid():N}",
            TablePrefix = $"avtobus_{Guid.NewGuid():N}_",
            BatchSize = 8,
            ReclaimTimeout = TimeSpan.FromSeconds(30),
            ListenTimeout = TimeSpan.FromMilliseconds(500),
        };

        var transport = new SqlTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(30);
}
