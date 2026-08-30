using AvtoBus.Kafka;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для Kafka-транспорта (док 18 §7, идея 98). Требует реального
/// брокера: тесты пропускаются, если переменная окружения AVTOBUS_KAFKA_BOOTSTRAP
/// не задана. В CI задаётся адрес инфраструктуры (напр. kafka:9092).
/// Топики создаются брокером автоматически (auto.create.topics.enable) при первой отправке.
/// </summary>
public sealed class KafkaTransportConformanceTests : TransportConformanceTests
{
    private const string EnvBootstrap = "AVTOBUS_KAFKA_BOOTSTRAP";

    public KafkaTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvBootstrap)))
            Assert.Skip("Kafka-брокер недоступен: задайте AVTOBUS_KAFKA_BOOTSTRAP");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var bootstrap = Environment.GetEnvironmentVariable(EnvBootstrap);
        var options = new KafkaOptions
        {
            BootstrapServers = string.IsNullOrEmpty(bootstrap) ? "localhost:9092" : bootstrap,
            ClientId = $"avtobus-conformance-{Guid.NewGuid():N}",
            ExactlyOnce = false,
            BackpressureThreshold = 8,
        };

        var transport = new KafkaTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(30);
}
