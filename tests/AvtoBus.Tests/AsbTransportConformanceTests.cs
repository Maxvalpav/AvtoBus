using AvtoBus.AzureServiceBus;

namespace AvtoBus.Tests;

/// <summary>
/// Conformance-прогон для Azure Service Bus-транспорта (док 18 §7, идея 98). Требует реального
/// namespace: тесты пропускаются, если переменная окружения AVTOBUS_ASB_CONNECTION не задана.
/// В CI задаётся строка подключения к service bus (Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...).
/// </summary>
public sealed class AsbTransportConformanceTests : TransportConformanceTests
{
    private const string EnvUrl = "AVTOBUS_ASB_CONNECTION";

    public AsbTransportConformanceTests()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvUrl)))
            Assert.Skip("Azure Service Bus недоступен: задайте AVTOBUS_ASB_CONNECTION");
    }

    protected override Task<ITransport> CreateAsync()
    {
        var connection = Environment.GetEnvironmentVariable(EnvUrl)!;
        var options = new AsbOptions
        {
            ConnectionString = connection,
            Name = $"avtobus-conformance-{Guid.NewGuid():N}",
            PrefetchCount = 8,
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(10),
        };

        var transport = new AsbTransport(options);
        return Task.FromResult<ITransport>(transport);
    }

    protected override TimeSpan Timeout => TimeSpan.FromSeconds(60);
}
