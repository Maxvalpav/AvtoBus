using System.Diagnostics.Metrics;
using AvtoBus.Observability;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Датчики каждого шага пайплайна — «водопад» обработки (идея 334).</summary>
public class PipelineStepMetricTests
{
    [Fact]
    public async Task Processing_emits_step_duration_samples_per_middleware()
    {
        var steps = new System.Collections.Concurrent.ConcurrentQueue<(string Step, string Type, double Ms)>();
        using var meterListener = new MeterListener();
        meterListener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            var tagsArray = tags.ToArray();
            var step = tagsArray.FirstOrDefault(t => t.Key == "messaging.avtobus.pipeline.step").Value?.ToString() ?? "?";
            var type = tagsArray.FirstOrDefault(t => t.Key == "messaging.message.type").Value?.ToString() ?? "?";
            steps.Enqueue((step, type, value));
        });
        meterListener.EnableMeasurementEvents(BusTelemetry.PipelineStepDuration);
        meterListener.Start();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Subscribe<OrderPlaced>((_, _) => Task.CompletedTask));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 10m));

        // Recorder ставится харнессом автоматически — ждём именно его шаг: чужой шаг из
        // параллельного харнесса не должен считаться нашим (чанолевая память step-двумерной).
        Assert.True(await harness.WaitUntilAsync(
            () => steps.Any(s => s.Step == "RecordingMiddleware" && s.Type == "OrderPlaced"),
            TimeSpan.FromSeconds(10)),
            "no pipeline step samples. Got: " + string.Join(", ", steps));
    }
}
