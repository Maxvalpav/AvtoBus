using AvtoBus.EventSourcing;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public class GdprReportServiceTests
{
    private static (GdprReportService Service, InMemorySubjectKeyRing Ring, InMemoryEventStore Store) Build()
    {
        var store = EsFixture.Store();
        var ring = new InMemorySubjectKeyRing();
        var configurator = new SubjectEncryptionConfigurator();
        configurator.PerSubject<AccountOpened>(e => e.AccountId.ToString(), e => e.Holder);

        var protection = new SubjectDataProtection(ring, configurator);
        var serializer = new EncryptingEventSerializer(EsFixture.Serializer(), protection);
        store = EsFixture.Store(serializer);

        var service = new GdprReportService(store, protection, ring);
        return (service, ring, store);
    }

    [Fact]
    public async Task Report_lists_all_subject_events_with_readable_pii()
    {
        var (service, _, store) = Build();
        var subject = Guid.NewGuid();
        var other = Guid.NewGuid();

        await store.AppendAsync(subject, "account",
            [new EventToAppend
            {
                Payload = new AccountOpened(subject, "Ann", 100m),
                EventType = "contracts.account-opened",
            }], 0);
        await store.AppendAsync(other, "account",
            [new EventToAppend
            {
                Payload = new AccountOpened(other, "Bob", 200m),
                EventType = "contracts.account-opened",
            }], 0);

        var report = await service.BuildReportAsync(subject.ToString());

        var occurrence = Assert.Single(report.Events);
        Assert.Equal("contracts.account-opened", occurrence.EventType);
        Assert.True(occurrence.PiiReadable);
        Assert.Empty(report.Forgotten);
    }

    [Fact]
    public async Task Report_marks_pii_unreadable_after_forget()
    {
        var (service, ring, store) = Build();
        var subject = Guid.NewGuid();

        await store.AppendAsync(subject, "account",
            [new EventToAppend
            {
                Payload = new AccountOpened(subject, "Ann", 100m),
                EventType = "contracts.account-opened",
            }], 0);

        ring.Forget(subject.ToString());

        var report = await service.BuildReportAsync(subject.ToString());

        Assert.Empty(report.Events);
        var forgotten = Assert.Single(report.Forgotten);
        Assert.False(forgotten.PiiReadable);
    }

    [Fact]
    public async Task Report_ignores_events_of_other_subjects_and_unconfigured_types()
    {
        var (service, _, store) = Build();
        var subject = Guid.NewGuid();
        var other = Guid.NewGuid();

        await store.AppendAsync(subject, "account",
            [new EventToAppend
            {
                Payload = new MoneyDeposited(subject, 5m),
                EventType = "contracts.money-deposited",
            }], 0);
        await store.AppendAsync(other, "account",
            [new EventToAppend
            {
                Payload = new AccountOpened(other, "Bob", 200m),
                EventType = "contracts.account-opened",
            }], 0);

        var report = await service.BuildReportAsync(subject.ToString());

        Assert.Empty(report.Events); // money-deposited не зарегистрирован в crypto-shredding
        Assert.Empty(report.Forgotten);
    }
}
