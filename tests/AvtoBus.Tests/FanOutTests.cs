using System.Collections.Concurrent;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Fan-out (идея 9): событие обрабатывается всеми подписчиками своего типа,
/// а не только первым. Проверяется через независимые счётчики в каждом хендлере.
/// </summary>
public class FanOutTests
{
    [Fact]
    public async Task Event_is_delivered_to_every_registered_consumer()
    {
        var probe = new FanOutProbe();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddContract<OrderPlaced>();
                bus.AddConsumer<FirstFanOutConsumer>();
                bus.AddConsumer<SecondFanOutConsumer>();
                bus.AddConsumer<ThirdFanOutConsumer>();
            },
            services => services.AddSingleton(probe));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 42m));

        Assert.True(await harness.WaitUntilAsync(
            () => probe.CountOf("first") == 1 && probe.CountOf("second") == 1 && probe.CountOf("third") == 1),
            "Не все подписчики получили событие");
    }
}

/// <summary>Сколько сообщений увидел каждый подписчик.</summary>
public sealed class FanOutProbe
{
    private readonly ConcurrentDictionary<string, int> _counts = new();

    public void Inc(string consumer) => _counts.AddOrUpdate(consumer, 1, static (_, value) => value + 1);

    public int CountOf(string consumer) => _counts.TryGetValue(consumer, out var count) ? count : 0;
}

public sealed class FirstFanOutConsumer(FanOutProbe probe) : IConsumer<OrderPlaced>
{
    public Task ConsumeAsync(ConsumeContext<OrderPlaced> context)
    {
        probe.Inc("first");
        return Task.CompletedTask;
    }
}

public sealed class SecondFanOutConsumer(FanOutProbe probe) : IConsumer<OrderPlaced>
{
    public Task ConsumeAsync(ConsumeContext<OrderPlaced> context)
    {
        probe.Inc("second");
        return Task.CompletedTask;
    }
}

public sealed class ThirdFanOutConsumer(FanOutProbe probe) : IConsumer<OrderPlaced>
{
    public Task ConsumeAsync(ConsumeContext<OrderPlaced> context)
    {
        probe.Inc("third");
        return Task.CompletedTask;
    }
}
