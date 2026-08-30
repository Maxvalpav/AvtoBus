namespace AvtoBus.Tests.EventSourcing.Contracts;

// Space of test events: конвенция именования даёт имена вида "contracts.account-opened".

public sealed record AccountOpened(Guid AccountId, string Holder, decimal InitialBalance) : EsTestEvent;
public sealed record MoneyDeposited(Guid AccountId, decimal Amount) : EsTestEvent;

// Вторая версия: появляется поле Currency — сценарий upcasting v1 -> v2.
public sealed record AccountOpenedV2(Guid AccountId, string Holder, decimal InitialBalance, string Currency) : EsTestEvent;

public abstract record EsTestEvent;
