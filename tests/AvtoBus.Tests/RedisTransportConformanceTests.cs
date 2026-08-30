using AvtoBus.Redis;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для Redis Streams-транспорта (док 18 §7, идея 98). Требует реального
/// сервера: тесты пропускаются, если переменная окружения AVTOBUS_REDIS_URL не задана.
/// В CI задаётся адрес инфраструктуры (напр. redis:6379).
/// </summary>
public sealed class RedisTransportConformanceTests : TransportConformanceTests
{
    private const string EnvUrl = "AVTOBUS_REDIS_URL";

    public RedisTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvUrl)))
            Assert.Skip("Redis-сервер недоступен: задайте AVTOBUS_REDIS_URL");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var url = Environment.GetEnvironmentVariable(EnvUrl);
        var options = new RedisOptions
        {
            Configuration = string.IsNullOrEmpty(url) ? "localhost:6379" : url,
            Name = $"avtobus-conformance-{Guid.NewGuid():N}",
            MaxStreamLength = 100_000,
        };

        var transport = new RedisTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(30);
}
