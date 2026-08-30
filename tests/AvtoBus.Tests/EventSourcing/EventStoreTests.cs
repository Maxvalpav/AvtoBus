using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Projections;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

/// <summary>
/// Базовый аксессор: стора + сериализатор, собранные по-минимуму без DI.
/// </summary>
public static class EsFixture
{
    public static JsonEventSerializer Serializer(params Type[] extra)
    {
        var types = new List<Type>(extra)
        {
            typeof(AccountOpened),
            typeof(MoneyDeposited),
            typeof(AccountOpenedV2),
        };
        return new JsonEventSerializer(types);
    }

    public static InMemoryEventStore Store(IEventSerializer? serializer = null)
        => new(serializer ?? Serializer());
}

public class EventStoreTests
{
    [Fact]
    public async Task Append_and_read_roundtrips_versions_and_sequences()
    {
        var store = EsFixture.Store();
        var stream = Guid.NewGuid();

        var r1 = await store.AppendAsync(stream, "account",
            [new EventToAppend { Payload = new AccountOpened(stream, "Ann", 100m), EventType = "contracts.account-opened" }],
            0);

        Assert.Equal(1, r1.NewVersion);
        Assert.Equal(1, r1.FirstSequence);
        Assert.Equal(1, r1.LastSequence);

        var r2 = await store.AppendAsync(stream, "account",
            [new EventToAppend { Payload = new MoneyDeposited(stream, 50m), EventType = "contracts.money-deposited" }],
            r1.NewVersion);

        Assert.Equal(2, r2.NewVersion);

        var read = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync(stream))
            read.Add(e);

        Assert.Equal(2, read.Count);
        Assert.Equal(1, read[0].Version);
        Assert.Equal(2, read[1].Version);
        Assert.Equal(r1.FirstSequence, read[0].GlobalSequence);
        Assert.Equal(r2.LastSequence, read[1].GlobalSequence);

        var head = await store.GetHeadSequenceAsync();
        Assert.Equal(2, head);
    }

    [Fact]
    public async Task Append_expected_version_conflict_throws_concurrency_exception()
    {
        var store = EsFixture.Store();
        var stream = Guid.NewGuid();

        await store.AppendAsync(stream, "account", [Event(stream, "open")], expectedVersion: 0);

        // Повторная запись с той же ожидаемой версией — конфликт.
        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(stream, "account", [Event(stream, "open")], expectedVersion: 0).AsTask());

        Assert.Equal(stream, ex.StreamId);
        Assert.Equal(0, ex.ExpectedVersion);
        Assert.Equal(1, ex.ActualVersion);
    }

    [Fact]
    public async Task Append_to_nonexistent_stream_creates_new_version()
    {
        var store = EsFixture.Store();
        var stream = Guid.NewGuid();

        var result = await store.AppendAsync(
            stream, "account", [Event(stream, "open")], expectedVersion: 0);

        Assert.Equal(1, result.NewVersion);
        var meta = await store.GetStreamAsync(stream);
        Assert.NotNull(meta);
        Assert.Equal("account", meta.StreamType);
        Assert.False(meta.IsArchived);
    }

    [Fact]
    public async Task Read_all_and_category_feed_projections_in_order()
    {
        var store = EsFixture.Store();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await store.AppendAsync(a, "account", [Event(a, "open")], 0);
        await store.AppendAsync(b, "account", [Event(b, "open")], 0);
        await store.AppendAsync(a, "account", [Event(a, "deposit")], 1);

        var all = new List<StoredEvent>();
        await foreach (var e in store.ReadAllAsync(0))
            all.Add(e);

        Assert.Equal(3, all.Count);
        Assert.Equal(new[] { a, b, a }, all.Select(e => e.StreamId));

        // Категория "account" — те же события (оба стрима).
        var category = new List<StoredEvent>();
        await foreach (var e in store.ReadCategoryAsync("account"))
            category.Add(e);
        Assert.Equal(3, category.Count);

        // Фильтр по типу.
        var deposits = new List<StoredEvent>();
        await foreach (var e in store.ReadAllAsync(0, eventTypeFilter: ["contracts.money-deposited"]))
            deposits.Add(e);
        Assert.Single(deposits);
    }

    [Fact]
    public async Task Snapshot_and_archive_lifecycle()
    {
        var store = EsFixture.Store();
        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Event(stream, "open")], 0);

        await store.SaveSnapshotAsync(new StoredSnapshot
        {
            StreamId = stream,
            Version = 1,
            StateType = typeof(AccountAggregate).FullName!,
            Data = new byte[] { 1, 2, 3 },
            CreatedAt = DateTimeOffset.UtcNow,
        });

        var snap = await store.LoadSnapshotAsync(stream);
        Assert.NotNull(snap);
        Assert.Equal(new byte[] { 1, 2, 3 }, snap.Data.ToArray());

        await store.ArchiveStreamAsync(stream);
        var meta = await store.GetStreamAsync(stream);
        Assert.NotNull(meta);
        Assert.True(meta.IsArchived);
    }

    private static EventToAppend Event(Guid id, string kind) => new()
    {
        Payload = kind == "open" ? new AccountOpened(id, "Ann", 0m) : new MoneyDeposited(id, 10m),
        EventType = kind == "open" ? "contracts.account-opened" : "contracts.money-deposited",
    };
}
