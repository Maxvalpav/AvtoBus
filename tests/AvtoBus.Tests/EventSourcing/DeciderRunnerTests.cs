using AvtoBus.EventSourcing;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public sealed record CreateAccount(string Holder, decimal Initial);
public sealed record Deposit(decimal Amount);

/// <summary>Функциональный decider: чистая логика без DI/IO (идея 253).</summary>
public sealed class BankDecider : IDecider<BankState, object, EsTestEvent>
{
    public BankState Initial => new();

    public IEnumerable<EsTestEvent> Decide(BankState state, object command)
    {
        switch (command)
        {
            case CreateAccount create when state.IsEmpty:
                yield return new AccountOpened(Guid.NewGuid(), create.Holder, create.Initial);
                break;
            case Deposit { Amount: > 0 } deposit:
                yield return new MoneyDeposited(state.Id, deposit.Amount);
                break;
        }
    }

    public BankState Evolve(BankState state, EsTestEvent @event)
    {
        switch (@event)
        {
            case AccountOpened a:
                state.Id = a.AccountId;
                state.Balance = a.InitialBalance;
                break;
            case MoneyDeposited d:
                state.Balance += d.Amount;
                state.Id = d.AccountId;
                break;
        }
        return state;
    }
}

public sealed class BankState
{
    public Guid Id { get; set; }
    public decimal Balance { get; set; }
    public bool IsEmpty => Id == Guid.Empty;
}

public class DeciderRunnerTests
{
    [Fact]
    public async Task Decide_appends_and_evolves_across_runs()
    {
        var store = EsFixture.Store();
        var runner = new DeciderRunner<BankState, object, EsTestEvent>(
            new BankDecider(), store, EsFixture.Serializer(), streamType: "account");

        var id = Guid.NewGuid();
        var r1 = await runner.HandleAsync(id, new CreateAccount("Ann", 100m));
        Assert.Equal(1, r1.NewVersion);

        var r2 = await runner.HandleAsync(id, new Deposit(50m));
        Assert.Equal(2, r2.NewVersion);

        var final = await runner.HandleAsync(id, new Deposit(1m));
        Assert.Equal(3, final.NewVersion);

        var events = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync(id))
            events.Add(e);

        Assert.Equal(3, events.Count);
    }

    [Fact]
    public async Task Decide_rejects_command_on_terminal_state()
    {
        var store = EsFixture.Store();
        var runner = new DeciderRunner<BankState, object, EsTestEvent>(
            new OnceDecider(), store, EsFixture.Serializer(), streamType: "account");

        var id = Guid.NewGuid();
        await runner.HandleAsync(id, new CreateAccount("Ann", 100m));
        // Терминальное состояние: вторая команда не порождает событий.
        var result = await runner.HandleAsync(id, new CreateAccount("Ann", 100m));
        Assert.Equal(0, result.LastSequence);
    }

    private sealed class OnceDecider : IDecider<BankState, object, EsTestEvent>
    {
        public BankState Initial => new();
        public bool IsTerminal(BankState state) => !state.IsEmpty;
        public IEnumerable<EsTestEvent> Decide(BankState state, object command)
        {
            if (command is CreateAccount) yield return new AccountOpened(Guid.NewGuid(), "x", 1m);
        }
        public BankState Evolve(BankState state, EsTestEvent @event)
        {
            if (@event is AccountOpened a) state.Id = a.AccountId;
            return state;
        }
    }
}
