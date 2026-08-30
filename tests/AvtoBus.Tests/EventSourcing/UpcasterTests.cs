using AvtoBus.EventSourcing;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

/// <summary>
/// Миграция версий событий: v1 без валюты → v2 с валютой (идея 252). Chain читает
/// (EventType, SchemaVersion) и применяет по цепочке при чтении.
/// </summary>
public class UpcasterTests
{
    private const string EventType = "contracts.account-opened";

    [Fact]
    public void Chain_applies_v1_to_v2()
    {
        var chain = new UpcasterChain([new AccountOpenedV1ToV2()]);

        object result = chain.Upcast(
            new AccountOpenedV1(Guid.NewGuid(), "Ann", 100m), EventType, schemaVersion: 1);

        var v2 = Assert.IsType<AccountOpenedV2>(result);
        Assert.Equal("RUB", v2.Currency);
        Assert.Equal(100m, v2.InitialBalance);
    }

    [Fact]
    public void Chain_skips_when_event_is_already_latest()
    {
        var chain = new UpcasterChain([new AccountOpenedV1ToV2()]);

        var v2 = new AccountOpenedV2(Guid.NewGuid(), "Ann", 0m, "RUB");
        object result = chain.Upcast(v2, EventType, schemaVersion: 2);

        Assert.Same(v2, result);
    }

    private sealed record AccountOpenedV1(Guid AccountId, string Holder, decimal InitialBalance);

    private sealed class AccountOpenedV1ToV2 : Upcaster<AccountOpenedV1, AccountOpenedV2>
    {
        public override string EventType => UpcasterTests.EventType;

        public override int FromVersion => 1;

        public override AccountOpenedV2 Upcast(AccountOpenedV1 old)
            => new(old.AccountId, old.Holder, old.InitialBalance, "RUB");
    }
}
