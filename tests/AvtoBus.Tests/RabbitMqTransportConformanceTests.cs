using AvtoBus.RabbitMq;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для RabbitMQ-транспорта (док 10 §7, идея 98). Требует реального
/// брокера: тесты пропускаются, если переменная окружения AVTOBUS_RABBIT_URL не задана.
/// В CI задаётся адрес инфраструктуры (напр. amqp://guest:guest@rabbitmq:5672/).
/// Топология (очереди/exchange) создаётся транспортом автоматически.
/// </summary>
public sealed class RabbitMqTransportConformanceTests : TransportConformanceTests
{
    private const string EnvUrl = "AVTOBUS_RABBIT_URL";

    public RabbitMqTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvUrl)))
            Assert.Skip("RabbitMQ-брокер недоступен: задайте AVTOBUS_RABBIT_URL");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var url = Environment.GetEnvironmentVariable(EnvUrl);
        var options = new RabbitMqOptions
        {
            ConnectionString = string.IsNullOrEmpty(url) ? "amqp://guest:guest@localhost:5672/" : url,
            ClientProvidedName = $"avtobus-conformance-{Guid.NewGuid():N}",
            UseQuorumQueues = true,
            UseDeadLetterExchange = true,
        };

        var transport = new RabbitMqTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(30);
}
