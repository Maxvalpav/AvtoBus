using AvtoBus.Nats;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для NATS/JetStream-транспорта (док 18 §7, идея 98). Требует реального
/// сервера: тесты пропускаются, если переменная окружения AVTOBUS_NATS_URL не задана.
/// В CI задаётся адрес инфраструктуры (напр. nats://nats:4222).
/// </summary>
public sealed class NatsTransportConformanceTests : TransportConformanceTests
{
    private const string EnvUrl = "AVTOBUS_NATS_URL";

    public NatsTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvUrl)))
            Assert.Skip("NATS-сервер недоступен: задайте AVTOBUS_NATS_URL");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var url = Environment.GetEnvironmentVariable(EnvUrl);
        var options = new NatsOptions
        {
            Url = string.IsNullOrEmpty(url) ? "nats://localhost:4222" : url,
            Name = $"avtobus-conformance-{Guid.NewGuid():N}",
            StorageType = "memory",
            MaxMsgsPerStream = 100_000,
        };

        var transport = new NatsTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(30);
}
