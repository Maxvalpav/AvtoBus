using System.Diagnostics.Metrics;
using AvtoBus.Configuration;
using AvtoBus.Observability;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Защита от «раздувания» контекста через хопы (идея 313).</summary>
public class HeaderLimitTests
{
    private static (EnvelopeFactory Factory, BusOptions Options) Create(int maxBytes = 16 * 1024, int maxCount = 64, int maxHops = 50)
    {
        var options = new BusOptions
        {
            MaxHeaderBytes = maxBytes,
            MaxHeaderCount = maxCount,
            MaxHops = maxHops,
        };

        var registry = MessageRegistry.Build([typeof(Contracts.OrderPlaced)]);
        var factory = new EnvelopeFactory(options, registry, TimeProvider.System);
        return (factory, options);
    }

    private static Envelope EnvelopeWithHops(int hops) => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "orders.order-placed",
        Body = "{}"u8.ToArray(),
        SentAt = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string> { [BusHeaders.Hops] = hops.ToString(System.Globalization.CultureInfo.InvariantCulture) },
    };

    [Fact]
    public void Cascade_increments_hop_count()
    {
        var (factory, _) = Create();
        var parent = EnvelopeWithHops(4);

        var child = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), null, parent);

        Assert.Equal("5", child.Header(BusHeaders.Hops));
    }

    [Fact]
    public void Hops_above_limit_stop_propagating_baggage()
    {
        var (factory, _) = Create(maxHops: 3);

        var truncated = new List<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "avtobus.headers.truncated")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => truncated.Add(value.ToString()));
        listener.Start();

        // Родитель уже на лимите хопов: наследуемые заголовки не должны пережить каскад.
        var parent = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "orders.order-placed",
            Body = "{}"u8.ToArray(),
            SentAt = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>
            {
                [BusHeaders.Hops] = "3",
                ["x-baggage"] = "heavy",
                ["x-continuation"] = "tail",
            },
        };

        var child = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), null, parent);

        Assert.Null(child.Header("x-baggage"));
        Assert.Null(child.Header("x-continuation"));
        Assert.Null(child.Header(BusHeaders.Hops));

        // Обрезание зафиксировано в метрике.
        Assert.Contains(truncated, v => v == "1");
        listener.Dispose();
    }

    [Fact]
    public void Byte_limit_trims_biggest_headers()
    {
        var (factory, _) = Create(maxBytes: 64);

        var options = new SendOptions()
            .WithHeader("x-small", "tiny")
            .WithHeader("x-fat", new string('x', 200));

        var envelope = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), options, null);

        // Жирный заголовок убран, мелкий пережил.
        Assert.Null(envelope.Header("x-fat"));
        Assert.NotNull(envelope.Header("x-small"));
    }

    [Fact]
    public void Count_limit_stops_after_max_headers()
    {
        var (factory, _) = Create(maxCount: 2);

        var options = new SendOptions()
            .WithHeader("a", "1")
            .WithHeader("b", "2")
            .WithHeader("c", "3")
            .WithHeader("d", "4");

        var envelope = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), options, null);

        var count = envelope.Headers.Count;
        Assert.True(count <= 2, $"expected <= 2 headers, got {count}");
    }
}
