using System.Diagnostics.Metrics;
using AvtoBus.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

public class QueueDepthMetricTests
{
    [Fact]
    public async Task Queue_depth_metric_and_dlq_size_metric_report_depths()
    {
        var queueDepth = new List<(string Queue, int Depth)>();
        var dlqSize = new List<(string Queue, int Depth)>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAvtoBus(bus => bus.UseInMemory());
        var provider = services.BuildServiceProvider();

        var transport = provider.GetRequiredService<InMemoryTransport>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument is ObservableGauge<int>
                { Name: "avtobus.queue.depth" or "avtobus.dlq.size" })
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((instrument, value, tags, state) =>
        {
            var queue = tags.ToArray()
                .FirstOrDefault(t => t.Key == "avtobus.queue.name").Value?.ToString() ?? "?";
            var entry = (queue, value);
            if (instrument.Name == "avtobus.queue.depth")
                queueDepth.Add(entry);
            else if (instrument.Name == "avtobus.dlq.size")
                dlqSize.Add(entry);
        });
        listener.Start();

        var gauges = provider.GetServices<ObservableGauge<int>>().ToList();
        Assert.Contains(gauges, g => g.Name == "avtobus.queue.depth");
        Assert.Contains(gauges, g => g.Name == "avtobus.dlq.size");

        await transport.SendAsync(Make(), TransportDestination.Queue("orders"));
        await transport.SendAsync(Make(), TransportDestination.Queue("orders"));
        await transport.SendAsync(Make(), TransportDestination.Queue("payments"));
        await transport.SendAsync(Make(), TransportDestination.Queue("orders.error"));

        listener.RecordObservableInstruments();
        await Task.Delay(30);

        var debug = string.Join(" | ", queueDepth.Select(c => $"{c.Queue}={c.Depth}"));
        Assert.True(
            queueDepth.Any(m => m.Queue == "orders" && m.Depth == 2),
            "orders depth 2 not captured. Got: [" + debug + "]");
        Assert.True(
            queueDepth.Any(m => m.Queue == "payments" && m.Depth == 1),
            "payments depth 1 not captured. Got: [" + debug + "]");

        var dlqDebug = string.Join(" | ", dlqSize.Select(c => $"{c.Queue}={c.Depth}"));
        Assert.True(
            dlqSize.Any(m => m.Queue == "orders.error" && m.Depth == 1),
            "orders.error in DLQ gauge missing. Got: [" + dlqDebug + "]");
        Assert.DoesNotContain(dlqSize, m => m.Queue == "orders");

        listener.Dispose();
        await provider.DisposeAsync();
    }

    private static Envelope Make() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "test.v1",
        Body = "{}"u8.ToArray(),
        SentAt = DateTimeOffset.UtcNow,
    };
}
