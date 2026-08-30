using System.Collections.Concurrent;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Упорядочивание по ключу (идея 25): сообщения одного ключа обрабатываются строго
/// последовательно даже при параллельной обработке разных ключей.
/// </summary>
public class PartitionOrderTests
{
    [Fact]
    public async Task Partitioned_consumer_preserves_order_per_key_under_concurrency()
    {
        var probe = new PartitionOrderProbe();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddContract<AccountEvent>();
                bus.AddConsumer<PartitionedAccountConsumer>();
                bus.Consumer<AccountEvent>().OrderedBy(e => e.AccountId, partitions: 4);
            },
            services => services.AddSingleton(probe));

        const int count = 40;
        for (var i = 1; i <= count; i++)
        {
            await harness.Bus.PublishAsync(new AccountEvent("A", i));
            await harness.Bus.PublishAsync(new AccountEvent("B", i));
            await harness.Bus.PublishAsync(new AccountEvent("C", i));
        }

        // Recorder фиксирует сообщение до хендлера, поэтому ждём, пока хендлер увидит все.
        Assert.True(await harness.WaitUntilAsync(
            () => probe.Observed.Values.Sum(queue => queue.Count) == count * 3),
            "Не все события обработаны");

        foreach (var (accountId, sequence) in probe.Observed)
            Assert.True(IsStrictlyIncreasing(sequence), $"Ключ {accountId}: порядок нарушен ({string.Join(",", sequence)})");
    }

    private static bool IsStrictlyIncreasing(IEnumerable<int> values)
    {
        var previous = int.MinValue;
        foreach (var value in values)
        {
            if (value <= previous)
                return false;
            previous = value;
        }

        return true;
    }
}

/// <summary>Фиксирует порядок, в котором хендлер реально увидел сообщения каждого ключа.</summary>
public sealed class PartitionOrderProbe
{
    public ConcurrentDictionary<string, ConcurrentQueue<int>> Observed { get; } = new();
}

public sealed class PartitionedAccountConsumer(PartitionOrderProbe probe) : IConsumer<AccountEvent>
{
    public async Task ConsumeAsync(ConsumeContext<AccountEvent> context)
    {
        var @event = context.Message;

        // Разная задержка по номеру последовательности: без партиционирования
        // параллельные слоты перемешали бы порядок завершения внутри ключа.
        await Task.Delay((@event.Sequence % 5) * 2, context.CancellationToken).ConfigureAwait(false);

        probe.Observed.GetOrAdd(@event.AccountId, _ => new()).Enqueue(@event.Sequence);
    }
}
