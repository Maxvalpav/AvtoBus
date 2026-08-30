using System.Diagnostics.Metrics;
using AvtoBus.Observability;
using AvtoBus.Runtime;
using AvtoBus.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Чёрный список на лету (идея 349): блокировка паттерна без рестарта, всё журналируется.</summary>
public class BlacklistTests
{
    [Fact]
    public void Registry_matches_exact_type_and_wildcard_prefix()
    {
        var registry = new BlacklistRegistry();

        Assert.False(registry.IsBlocked("Orders.Events.OrderPlaced"));

        registry.Block("Orders.Events.OrderPlaced");
        Assert.True(registry.IsBlocked("Orders.Events.OrderPlaced"));

        registry.Block("Orders.Events.OrderPaid");
        Assert.True(registry.IsBlocked("Orders.Events.OrderPaid"));
    }

    [Fact]
    public void Wildcard_blocks_whole_namespace()
    {
        var registry = new BlacklistRegistry();
        registry.Block("Orders.Events.*");

        Assert.True(registry.IsBlocked("Orders.Events.OrderPlaced"));
        Assert.True(registry.IsBlocked("Orders.Events.OrderPaid"));
        Assert.False(registry.IsBlocked("Billing.ChargeCard"));
    }

    [Fact]
    public void Unblock_removes_pattern_in_runtime()
    {
        var registry = new BlacklistRegistry();
        registry.Block("Orders.Events.OrderPlaced");
        Assert.True(registry.IsBlocked("Orders.Events.OrderPlaced"));

        registry.Unblock("Orders.Events.OrderPlaced");
        Assert.False(registry.IsBlocked("Orders.Events.OrderPlaced"));
    }

    private sealed class BlockedCounter : IDisposable
    {
        private readonly MeterListener _listener = new();
        public long Count;

        public BlockedCounter()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name == "avtobus.blacklist.blocked")
                    l.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref Count, value));
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }

    [Fact]
    public async Task Blocked_pattern_is_dropped_before_handler()
    {
        using var counter = new BlockedCounter();
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.UseBlacklist();
            bus.Subscribe<Contracts.OrderArchived>((_, _) => { Interlocked.Increment(ref handled); return Task.CompletedTask; });
        });

        var registry = harness.Services.GetRequiredService<BlacklistRegistry>();
        registry.Block("OrderArchived");

        await harness.Bus.PublishAsync(new Contracts.OrderArchived(Guid.NewGuid()));

        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref counter.Count) >= 1, TimeSpan.FromSeconds(5)),
            "blocked message did not reach the metric");

        Assert.Equal(0, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task Unblocked_pattern_is_processed_again()
    {
        using var counter = new BlockedCounter();
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.UseBlacklist();
            bus.Subscribe<Contracts.OrderArchived>((_, _) => { Interlocked.Increment(ref handled); return Task.CompletedTask; });
        });

        var registry = harness.Services.GetRequiredService<BlacklistRegistry>();
        registry.Block("OrderArchived");

        await harness.Bus.PublishAsync(new Contracts.OrderArchived(Guid.NewGuid()));
        // Ждём, пока первое сообщение реально упало в blacklist (пока оно не вычитано,
        // unblock последует до его обработки, и handled может успеть стать 2 — флаки).
        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref counter.Count) >= 1, TimeSpan.FromSeconds(5)),
            "первое сообщение не достигло blacklist");
        Assert.Equal(0, Volatile.Read(ref handled));

        registry.Unblock("OrderArchived");

        await harness.Bus.PublishAsync(new Contracts.OrderArchived(Guid.NewGuid()));
        Assert.True(await harness.WaitUntilAsync(
            () => Volatile.Read(ref handled) == 1,
            TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Initial_blacklist_configuration_applies_at_startup()
    {
        using var counter = new BlockedCounter();
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.UseBlacklist("OrderArchived");
            bus.Subscribe<Contracts.OrderArchived>((_, _) => { Interlocked.Increment(ref handled); return Task.CompletedTask; });
        });

        await harness.Bus.PublishAsync(new Contracts.OrderArchived(Guid.NewGuid()));

        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref counter.Count) >= 1, TimeSpan.FromSeconds(5)),
            "startup-blocked message did not reach the metric");
        Assert.Equal(0, Volatile.Read(ref handled));
    }
}
