using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;

namespace AvtoBus.Tests;

/// <summary>
/// Кардинальность метрик ограничена (идея 302): в тегах метрик не появляются
/// пер-сообщение идентификаторы — MessageId, CorrelationId, TenantId и т.п.
/// Теги приходят только из фиксированного набора (тип, назначение, исход).
/// </summary>
public class MetricsCardinalityTests
{
    private static readonly string[] AllowedMetricTagKeys =
    [
        "messaging.message.type",
        "messaging.destination.name",
        "messaging.avtobus.outcome",
        "messaging.avtobus.retry.kind",
        "messaging.avtobus.pipeline.step",
        "messaging.operation",
        // ProjectionLag (AvtoBus.EventSourcing): тег по имени зарегистрированной
        // проекции — фиксированный набор, как и pipeline.step.
        "projection",
    ];

    [Fact]
    public async Task Metric_tags_never_contain_high_cardinality_identifiers()
    {
        var observedTags = new ConcurrentDictionary<string, int>();

        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "AvtoBus")
                    l.EnableMeasurementEvents(instrument);
            };

            void Record(ReadOnlySpan<KeyValuePair<string, object?>> tags)
            {
                foreach (var tag in tags)
                    observedTags[tag.Key] = 0;
            }

            listener.SetMeasurementEventCallback<long>((_, _, tags, _) => Record(tags));
            listener.SetMeasurementEventCallback<double>((_, _, tags, _) => Record(tags));
            listener.SetMeasurementEventCallback<int>((_, _, tags, _) => Record(tags));
            listener.Start();

            await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
            {
                bus.AddContract<PlaceOrder>();
                bus.AddContract<OrderPlaced>();
                bus.AddConsumer<PlaceOrderMetricsConsumer>();
            });

            for (var i = 0; i < 5; i++)
                await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), $"customer-{i}", 10m + i));

            Assert.True(await harness.WaitForConsumedAsync<PlaceOrder>(5), "Сообщения не обработаны");

            // Даём callback'ам слушателя дозаписать теги завершающих метрик.
            await Task.Delay(200);
        }

        Assert.NotEmpty(observedTags);

        var unexpected = observedTags.Keys
            .Where(key => !AllowedMetricTagKeys.Contains(key, StringComparer.Ordinal))
            .OrderBy(key => key)
            .ToArray();

        Assert.Empty(unexpected);
    }
}

public sealed class PlaceOrderMetricsConsumer : IConsumer<PlaceOrder>
{
    public Task ConsumeAsync(ConsumeContext<PlaceOrder> context) => Task.CompletedTask;
}
