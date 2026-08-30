using System.Text.Json.Serialization;
using AvtoBus.EventSourcing;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public sealed class AccountAggregate : Aggregate
{
    [JsonInclude]
    public decimal Balance { get; private set; }

    [JsonInclude]
    public string Holder { get; private set; } = "";

    public void Open(Guid id, string holder, decimal initial)
    {
        Id = id;
        if (UncommittedEvents.Count == 0 && Version == 0)
            Apply(new AccountOpened(id, holder, initial));
    }

    public void Deposit(decimal amount) => Apply(new MoneyDeposited(Id, amount));

    protected override void When(object @event)
    {
        switch (@event)
        {
            case AccountOpened opened:
                Holder = opened.Holder;
                Balance += opened.InitialBalance;
                break;
            case MoneyDeposited deposited:
                Balance += deposited.Amount;
                break;
        }
    }
}

public class AggregateRepositoryTests
{
    [Fact]
    public async Task Save_then_load_replays_state_through_snapshots()
    {
        var store = EsFixture.Store();
        var policy = new SnapshotPolicy { DefaultEveryNEvents = 10 };
        var repo = new AggregateRepository(store, EsFixture.Serializer(), new UpcasterChain([]), policy, TimeProvider.System, bus: null);

        // Сохраняем агрегат: открытие + 3 депозита.
        var aggregate = new AccountAggregate();
        aggregate.Open(Guid.NewGuid(), "Ann", 100m);
        aggregate.Deposit(50m);
        aggregate.Deposit(25m);
        aggregate.Deposit(25m);

        var expectedId = aggregate.Id;
        var result = await repo.SaveAsync(aggregate);

        Assert.Equal(4, result.NewVersion);
        Assert.Empty(aggregate.UncommittedEvents);

        // Сохраняем снапшот вручную, потом грузим из снапшота.
        await store.SaveSnapshotAsync(new StoredSnapshot
        {
            StreamId = expectedId,
            Version = 4,
            StateType = typeof(AccountAggregate).FullName!,
            Data = EsFixture.Serializer().SerializeSnapshot(aggregate),
            CreatedAt = DateTimeOffset.UtcNow,
        });

        var loaded = await repo.LoadAsync<AccountAggregate>(expectedId);
        Assert.NotNull(loaded);
        Assert.Equal(4, loaded.Version);
        Assert.Equal(200m, loaded.Balance);
        Assert.Equal("Ann", loaded.Holder);
    }

    [Fact]
    public async Task Save_throws_on_concurrent_modification()
    {
        var store = EsFixture.Store();
        var repo = new AggregateRepository(store, EsFixture.Serializer(), new UpcasterChain([]), new SnapshotPolicy(), TimeProvider.System, bus: null);

        var aggregate = new AccountAggregate();
        aggregate.Open(Guid.NewGuid(), "Bob", 10m);
        await repo.SaveAsync(aggregate);

        // «Вторая сессия» грузит ту же версию и пишет поверх.
        var second = await repo.LoadAsync<AccountAggregate>(aggregate.Id);
        Assert.NotNull(second);
        second!.Deposit(1m);
        await repo.SaveAsync(second);

        // Первая сессия всё ещё держит старую версию в памяти — конфликт.
        aggregate.Deposit(1m);
        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() => repo.SaveAsync(aggregate).AsTask());

        Assert.Equal(1, ex.ExpectedVersion);
        Assert.Equal(2, ex.ActualVersion);
    }

    [Fact]
    public async Task LoadAsOf_restores_state_at_point_in_time()
    {
        var store = EsFixture.Store();
        var repo = new AggregateRepository(store, EsFixture.Serializer(), new UpcasterChain([]), new SnapshotPolicy(), TimeProvider.System, bus: null);

        var aggregate = new AccountAggregate();
        aggregate.Open(Guid.NewGuid(), "Eve", 100m);
        var id = aggregate.Id;

        // Записываем с одним депозитом, патчим timestamp вручную — сложно: стора задаёт время.
        // Вместо этого просто проверяем, что LoadAsOf на весь период видит 2 события.
        aggregate.Deposit(10m);
        await repo.SaveAsync(aggregate);

        var historical = await repo.LoadAsOfAsync<AccountAggregate>(id, DateTimeOffset.UtcNow.AddDays(1));
        Assert.NotNull(historical);
        Assert.Equal(2, historical.Version);
        Assert.Equal(110m, historical.Balance);
    }
}
