using System.Diagnostics.Metrics;
using AvtoBus.InMemory;
using AvtoBus.Observability;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

public class OutboxPendingMetricTests
{
    private sealed class FakeOutbox : IOutboxPendingProvider
    {
        public long OutboxPending { get; set; }
    }

    [Fact]
    public async Task Outbox_pending_gauge_reports_provider_value()
    {
        var captured = new List<long>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAvtoBus(bus => bus.UseInMemory());
        var outbox = new FakeOutbox { OutboxPending = 7 };
        services.AddSingleton<IOutboxPendingProvider>(outbox);
        var provider = services.BuildServiceProvider();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument is ObservableGauge<long> { Name: "avtobus.outbox.pending" })
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => captured.Add(value));
        listener.Start();

        var gauge = provider.GetServices<ObservableGauge<long>>().Single(g => g.Name == "avtobus.outbox.pending");

        listener.RecordObservableInstruments();
        await Task.Delay(30);

        Assert.Contains(captured, v => v == 7);

        listener.Dispose();
        await provider.DisposeAsync();
    }
}
