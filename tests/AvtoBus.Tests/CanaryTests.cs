using System.Diagnostics.Metrics;
using System.Diagnostics.Tracing;
using AvtoBus.Observability;
using AvtoBus.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Канарейка (идея 337): системное сообщение проходит всю цепочку и замеряет RTT.</summary>
public class CanaryTests
{
    private sealed class CanaryListener : EventListener
    {
        public readonly List<(int EventId, double? Rtt)> Completed = [];

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            if (eventSource.Name == "AvtoBus-Diagnostics")
                EnableEvents(eventSource, EventLevel.Verbose);
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (eventData.EventSource.Name == "AvtoBus-Diagnostics" && eventData.EventId == 6)
                Completed.Add((eventData.EventId, eventData.Payload?[0] is double rtt ? rtt : null));
        }
    }

    [Fact]
    public async Task Canary_flies_through_the_whole_chain_and_records_rtt()
    {
        using var listener = new CanaryListener();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .UseCanary(interval: TimeSpan.FromMilliseconds(100), timeout: TimeSpan.FromSeconds(5)));

        Assert.True(await harness.WaitUntilAsync(
            () => listener.Completed.Count >= 2,
            TimeSpan.FromSeconds(10)),
            "canary did not complete a round trip");
    }

    [Fact]
    public async Task Canary_publishes_rtt_histogram_samples()
    {
        var samples = new List<double>();
        using var meterListener = new MeterListener();
        meterListener.SetMeasurementEventCallback<double>((_, value, _, _) => samples.Add(value));
        meterListener.EnableMeasurementEvents(BusTelemetry.CanaryRtt);
        meterListener.Start();

        using var evtListener = new CanaryListener();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .UseCanary(interval: TimeSpan.FromMilliseconds(100), timeout: TimeSpan.FromSeconds(5)));

        Assert.True(await harness.WaitUntilAsync(
            () => samples.Count >= 2,
            TimeSpan.FromSeconds(10)),
            "no canary.rtt histogram samples. Got: " + string.Join(", ", samples));
    }
}
