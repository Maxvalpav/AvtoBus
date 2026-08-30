using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Streaming;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public class EventStreamTests
{
    private static readonly Guid Account = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static async Task<InMemoryEventStore> StoreWithDeposits(params decimal[] amounts)
    {
        var store = EsFixture.Store(EsFixture.Serializer(typeof(MoneyDeposited)));
        var version = 0;
        foreach (var amount in amounts)
        {
            var result = await store.AppendAsync(
                Account, "account",
                [
                    new EventToAppend
                    {
                        Payload = new MoneyDeposited(Account, amount),
                        EventType = "contracts.money-deposited",
                    },
                ],
                version);
            version = result.NewVersion;
        }
        return store;
    }

    [Fact]
    public async Task Stream_aggregates_sum_by_group_over_tumbling_window()
    {
        var store = await StoreWithDeposits(10m, 20m, 30m);

        WindowResult<MoneyDeposited>? delivered = null;
        var stream = store
            .Stream<MoneyDeposited>(EsFixture.Serializer(typeof(MoneyDeposited)))
            .Window(WindowStrategy.Tumbling(TimeSpan.FromHours(1)))
            .GroupBy(e => e.AccountId.ToString())
            .Aggregate(list => (double)list.Sum(e => e.Amount))
            .Into(r => delivered = r);

        var position = await stream.RunAsync();

        Assert.NotNull(delivered);
        Assert.Equal(60m, (decimal)delivered!.Groups[Account.ToString()].Sum!.Value);
        Assert.Equal(3, delivered.Groups[Account.ToString()].Count);
        Assert.Equal(20m, (decimal)delivered.Groups[Account.ToString()].Average!.Value);
        Assert.Equal(3, position);
    }

    [Fact]
    public async Task Stream_emits_one_result_per_group()
    {
        var store = await StoreWithDeposits(10m, 20m);

        var results = new List<WindowResult<MoneyDeposited>>();
        var stream = store
            .Stream<MoneyDeposited>(EsFixture.Serializer(typeof(MoneyDeposited)))
            .GroupBy(e => e.Amount > 15m ? "high" : "low")
            .Aggregate(list => (double)list.Sum(e => e.Amount))
            .Into(results.Add);

        await stream.RunAsync();

        var single = Assert.Single(results);
        Assert.Equal(2, single.Groups.Count);
        Assert.Equal(1, single.Groups["low"].Count);
        Assert.Equal(1, single.Groups["high"].Count);
    }

    [Fact]
    public async Task Stream_resumes_from_sequence()
    {
        var store = await StoreWithDeposits(10m);

        var delivered = new List<WindowResult<MoneyDeposited>>();
        var stream = store
            .Stream<MoneyDeposited>(EsFixture.Serializer(typeof(MoneyDeposited)))
            .GroupBy(e => "all")
            .Aggregate(list => list.Count)
            .Into(delivered.Add);

        var first = await stream.RunAsync();
        Assert.Equal(1, first);

        // Новые события после чекпоинта.
        await store.AppendAsync(
            Account, "account",
            [
                new EventToAppend
                {
                    Payload = new MoneyDeposited(Account, 5m),
                    EventType = "contracts.money-deposited",
                },
            ],
            1);

        var second = await stream.RunAsync();
        Assert.Equal(2, second);
    }
}
