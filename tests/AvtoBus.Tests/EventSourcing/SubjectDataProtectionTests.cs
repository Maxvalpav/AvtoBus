using AvtoBus.EventSourcing;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public class SubjectDataProtectionTests
{
    private static SubjectDataProtection Build(out InMemorySubjectKeyRing ring)
    {
        ring = new InMemorySubjectKeyRing();
        var configurator = new SubjectEncryptionConfigurator();
        configurator.PerSubject<AccountOpened>(
            e => e.AccountId.ToString(),
            e => e.Holder);
        return new SubjectDataProtection(ring, configurator);
    }

    [Fact]
    public void Protect_encrypts_holder_but_leaves_subject_open()
    {
        var protection = Build(out _);
        var evt = new AccountOpened(Guid.NewGuid(), "Ann", 100m);

        var bytes = protection.Protect(evt, "contracts.account-opened");
        var json = System.Text.Json.JsonDocument.Parse(bytes);

        // Поле-субъект остаётся открытым — это индекс.
        var subjectField = json.RootElement.GetProperty("AccountId");
        Assert.False(subjectField.ValueKind == System.Text.Json.JsonValueKind.Null);

        // Holder заменён на конверт {"$enc": ...}.
        var holder = json.RootElement.GetProperty("Holder");
        Assert.Equal(System.Text.Json.JsonValueKind.Object, holder.ValueKind);
        Assert.True(holder.TryGetProperty("$enc", out _));
    }

    [Fact]
    public void Unprotect_roundtrips_original_fields()
    {
        var protection = Build(out _);
        var accountId = Guid.NewGuid();
        var evt = new AccountOpened(accountId, "Ann", 100m);

        var bytes = protection.Protect(evt, "contracts.account-opened");
        var restored = (AccountOpened)protection.Unprotect(
            bytes, "contracts.account-opened", typeof(AccountOpened));

        Assert.Equal(accountId, restored.AccountId);
        Assert.Equal("Ann", restored.Holder);
        Assert.Equal(100m, restored.InitialBalance);
    }

    [Fact]
    public void Unprotect_returns_null_for_forgotten_subject()
    {
        var protection = Build(out var ring);
        var evt = new AccountOpened(Guid.NewGuid(), "Ann", 100m);

        var bytes = protection.Protect(evt, "contracts.account-opened");

        // «Право на забвение»: удаляем ключ субъекта.
        var subjectId = ((AccountOpened)evt).AccountId.ToString();
        ring.Forget(subjectId);

        var restored = (AccountOpened)protection.Unprotect(
            bytes, "contracts.account-opened", typeof(AccountOpened));

        Assert.Null(restored.Holder); // PII нечитаемо, но событие живо
        Assert.Equal(evt.AccountId, restored.AccountId);
    }

    [Fact]
    public void Events_without_encryption_config_pass_through()
    {
        var protection = Build(out _);
        var evt = new MoneyDeposited(Guid.NewGuid(), 50m);

        var bytes = protection.Protect(evt, "contracts.money-deposited");
        var restored = (MoneyDeposited)protection.Unprotect(
            bytes, "contracts.money-deposited", typeof(MoneyDeposited));

        Assert.Equal(50m, restored.Amount);
    }

    [Fact]
    public void TryGetSubjectId_works_without_key()
    {
        var protection = Build(out _);
        var accountId = Guid.NewGuid();
        var bytes = protection.Protect(new AccountOpened(accountId, "Ann", 100m), "contracts.account-opened");

        Assert.True(protection.TryGetSubjectId("contracts.account-opened", bytes, out var subjectId));
        Assert.Equal(accountId.ToString(), subjectId);
    }

    [Fact]
    public void Unprotected_event_type_reports_no_subject()
    {
        var protection = Build(out _);
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new MoneyDeposited(Guid.NewGuid(), 5m));

        Assert.False(protection.TryGetSubjectId("contracts.money-deposited", bytes, out _));
    }
}
