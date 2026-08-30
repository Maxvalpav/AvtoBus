using System.Diagnostics.Metrics;
using AvtoBus.Observability;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

public class MessageSizeMetricTests
{
    [Fact]
    public async Task Publish_and_consume_bytes_metrics_record_payload_size()
    {
        var raw = new System.Collections.Concurrent.ConcurrentBag<string>();

        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) =>
            raw.Add($"{instrument.Name}={value}"));

        listener.Start();
        listener.EnableMeasurementEvents(BusTelemetry.PublishBytes);
        listener.EnableMeasurementEvents(BusTelemetry.ConsumeBytes);

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<OrderPlacedConsumer>(),
            services => services.AddSingleton(new TaskCompletionSource<OrderPlaced>()));

        var message = new OrderPlaced(Guid.NewGuid(), 199.99m);
        await harness.Bus.PublishAsync(message);

        Assert.True(await harness.WaitForConsumedAsync<OrderPlaced>());

        var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message);
        Assert.True(await PollUntilAsync(() => raw.Contains($"avtobus.publish.bytes={body.Length}")),
            "publish bytes missing. raw: [" + string.Join(", ", raw) + "]");
        Assert.True(await PollUntilAsync(() => raw.Contains($"avtobus.consume.bytes={body.Length}")),
            "consume bytes missing. raw: [" + string.Join(", ", raw) + "]");

        listener.Dispose();
    }

    private static async Task<bool> PollUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(10).ConfigureAwait(false);
        }
        return condition();
    }
}
