using System.Text;
using AvtoBus.Configuration;
using AvtoBus.Runtime;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Режим CloudEvents 1.0 (аудит 03 §1.3): исходящие конверты несут ce-атрибуты
/// бинарного режима поверх собственных полей — интероп с Knative, Dapr, Event Grid.
/// </summary>
public class CloudEventsTests
{
    private static Envelope NewEnvelope() => new()
    {
        MessageId = Guid.Parse("11111111-2222-3333-4444-555555555555"),
        MessageType = "orders.order-placed",
        Body = Encoding.UTF8.GetBytes("{}"),
        SentAt = new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero),
    };

    [Fact]
    public void Apply_sets_spec_attributes()
    {
        var mapped = CloudEvents.Apply(NewEnvelope(), "orders-svc");

        Assert.Equal("1.0", mapped.Header("ce-specversion"));
        Assert.Equal("11111111-2222-3333-4444-555555555555", mapped.Header("ce-id"));
        Assert.Equal("orders.order-placed", mapped.Header("ce-type"));
        Assert.Equal("orders-svc", mapped.Header("ce-source"));
        Assert.Equal("2026-01-15T12:00:00.0000000Z", mapped.Header("ce-time"));
        Assert.Null(mapped.Header("traceparent"));
        // Собственные поля нетронуты.
        Assert.Equal("orders.order-placed", mapped.MessageType);
    }

    [Fact]
    public void Apply_propagates_traceparent_when_present()
    {
        var mapped = CloudEvents.Apply(
            NewEnvelope() with { TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" },
            "orders-svc");

        Assert.Equal("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", mapped.Header("traceparent"));
    }

    [Fact]
    public void Apply_keeps_explicit_app_headers()
    {
        var mapped = CloudEvents.Apply(
            NewEnvelope().WithHeader("ce-type", "custom.override"),
            "orders-svc");

        Assert.Equal("custom.override", mapped.Header("ce-type"));
        Assert.Equal("orders-svc", mapped.Header("ce-source"));
    }

    [Fact]
    public void UseCloudEvents_rejects_blank_source()
    {
        var bus = new BusConfigurator(new ServiceCollection(), new BusOptions());
        Assert.Throws<ArgumentException>(() => bus.UseCloudEvents(""));
        Assert.Throws<ArgumentException>(() => bus.UseCloudEvents("   "));
    }

    [Fact]
    public async Task Outbound_carries_ce_headers_end_to_end()
    {
        IReadOnlyDictionary<string, string>? seen = null;

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .UseCloudEvents("orders-svc")
                .Subscribe<OrderPlaced>(ctx =>
                {
                    seen = ctx.Envelope.Headers;
                    return Task.CompletedTask;
                }));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 100m));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (seen is null && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        Assert.NotNull(seen);
        Assert.Equal("1.0", seen["ce-specversion"]);
        Assert.Equal("contracts.order-placed", seen["ce-type"]);
        Assert.Equal("orders-svc", seen["ce-source"]);
        Assert.True(seen.ContainsKey("ce-id"));
        Assert.True(seen.ContainsKey("ce-time"));
    }

    [Fact]
    public async Task Disabled_by_default_no_ce_headers()
    {
        IReadOnlyDictionary<string, string>? seen = null;

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.Subscribe<OrderPlaced>(ctx =>
            {
                seen = ctx.Envelope.Headers;
                return Task.CompletedTask;
            }));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 100m));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (seen is null && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        Assert.NotNull(seen);
        Assert.False(seen.ContainsKey("ce-specversion"));
    }
}
