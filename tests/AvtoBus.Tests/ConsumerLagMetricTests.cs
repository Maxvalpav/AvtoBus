using System.Diagnostics.Metrics;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Observability;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AvtoBus.Tests;

public class ConsumerLagMetricTests
{
    [Fact]
    public async Task Runner_lag_counts_unconsumed_messages_in_queue()
    {
        var transport = new InMemoryTransport();
        await transport.SendAsync(Make(), TransportDestination.Queue("orders"));
        await transport.SendAsync(Make(), TransportDestination.Queue("orders"));

        var runner = new ConsumerRunner(
            new ConsumerSubscription(
                transport,
                new TransportSubscription(TransportDestination.Queue("orders"), "svc"),
                MessageType: null),
            processor: null!,
            new BusOptions(),
            TimeProvider.System,
            NullLogger.Instance);

        Assert.Equal(2, runner.Lag);

        await transport.DisposeAsync();
    }

    [Fact]
    public async Task Consumer_lag_gauge_reports_registered_provider_lags()
    {
        var captured = new List<(string Dest, long Lag)>();

        var fake = new FakeLagProvider();
        fake.Lags["orders-probe-a"] = 5;
        fake.Lags["orders-paid-probe-b"] = 11;

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAvtoBus(bus => bus.UseInMemory());
        services.AddSingleton<IConsumerLagProvider>(fake);
        var provider = services.BuildServiceProvider();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument is ObservableGauge<long> { Name: "avtobus.consumer.lag" })
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
        {
            // Meter глобальный: параллельные харнессы тоже регистрируют ObservableGauge, и их
            // провайдеры могут быть уже disposed — такие измерения пропускаем.
            try
            {
                var dest = tags.ToArray()
                    .FirstOrDefault(t => t.Key == "avtobus.destination.name").Value?.ToString() ?? "?";
                captured.Add((dest, value));
            }
            catch (ObjectDisposedException)
            {
            }
        });
        listener.Start();

        var gauges = provider.GetServices<ObservableGauge<long>>();
        Assert.Contains(gauges, g => g.Name == "avtobus.consumer.lag");

        // Глобальный Meter: RecordObservableInstruments дёргает и чужие ObservableGauge из
        // параллельных тестов, чьи провайдеры уже disposed — такие наблюдения пропускаем,
        // а наши уникальные имена собираем отдельным запуском.
        try
        {
            listener.RecordObservableInstruments();
        }
        catch (AggregateException ex) when (ex.InnerException is ObjectDisposedException or InvalidOperationException)
        {
        }

        await Task.Delay(30);

        if (!captured.Any(m => m.Dest == "orders-probe-a" && m.Lag == 5)
            || !captured.Any(m => m.Dest == "orders-paid-probe-b" && m.Lag == 11))
        {
            listener.RecordObservableInstruments();
            await Task.Delay(30);
        }

        var debug = string.Join(" | ", captured.Select(c => $"{c.Dest}={c.Lag}"));
        Assert.True(captured.Any(m => m.Dest == "orders-probe-a" && m.Lag == 5), "orders lag missing. Got: [" + debug + "]");
        Assert.True(captured.Any(m => m.Dest == "orders-paid-probe-b" && m.Lag == 11), "orders-paid lag missing. Got: [" + debug + "]");

        listener.Dispose();
    }

    private static Envelope Make() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "test.v1",
        Body = "{}"u8.ToArray(),
        SentAt = DateTimeOffset.UtcNow,
    };

    private sealed class FakeLagProvider : IConsumerLagProvider
    {
        public Dictionary<string, long> Lags { get; } = new(StringComparer.Ordinal);
        public IReadOnlyDictionary<string, long> ConsumerLags => Lags;
    }
}
