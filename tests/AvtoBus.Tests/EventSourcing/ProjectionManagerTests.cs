using System.Collections.Concurrent;
using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Projections;
using AvtoBus.Tests.EventSourcing.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public class ProjectionManagerTests
{
    private static ProjectionManager Build(IEventStore store, params IProjection[] projections)
        => new(
            store,
            projections,
            EsFixture.Serializer(),
            new UpcasterChain([]),
            NullLogger<ProjectionManager>.Instance);

    [Fact]
    public async Task Status_reports_lag_and_catchup()
    {
        var store = EsFixture.Store();
        var projection = new VersionedCountingProjection();
        var manager = Build(store, projection);

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Open(stream, "Ann")], 0);
        await store.AppendAsync(stream, "account", [Deposit(stream, 10m)], 1);

        var statuses = await manager.GetStatusAsync();
        var status = Assert.Single(statuses);

        Assert.Equal("counts", status.Name);
        Assert.Equal(0, status.Position);
        Assert.Equal(2, status.Head);
        Assert.Equal(2, status.Lag);
        Assert.Equal("lagging", status.State);
    }

    [Fact]
    public async Task Rebuild_replays_from_zero_to_head()
    {
        var store = EsFixture.Store();
        var projection = new VersionedCountingProjection();
        var manager = Build(store, projection);

        // Одно событие уже применено «вручную» с чекпоинтом 1.
        IVersionedProjection versioned = projection;
        await versioned.SaveCheckpointAsync(projection.Name, 1, default);
        projection.Record("contracts.account-opened");

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Open(stream, "Ann")], 0);
        await store.AppendAsync(stream, "account", [Deposit(stream, 10m)], 1);

        await manager.RebuildAsync("counts");

        Assert.Equal(2, projection.Position);
        Assert.Equal(1, projection.Counts["contracts.account-opened"]);
        Assert.Equal(1, projection.Counts["contracts.money-deposited"]);
    }

    [Fact]
    public async Task BuildVersion_builds_side_by_side_and_activate_switches_checkpoint()
    {
        var store = EsFixture.Store();
        var projection = new VersionedCountingProjection();
        var manager = Build(store, projection);

        // Активная v1 на чекпоинте 1 (одно событие).
        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Open(stream, "Ann")], 0);
        await store.AppendAsync(stream, "account", [Deposit(stream, 10m)], 1);
        IVersionedProjection versioned = projection;
        await versioned.SaveCheckpointAsync("counts", 1, default);
        projection.Record("contracts.account-opened");

        // Строим v2 с нуля — она должна догнать голову (2), не трогая активный чекпоинт.
        await manager.BuildVersionAsync("counts", 2);

        Assert.Equal(1, projection.Position); // активная версия не сдвинулась
        Assert.Equal(2, projection.Checkpoint("counts::v2"));

        // Активируем — чекпоинт основной версии переезжает на v2.
        await manager.ActivateVersionAsync("counts", 2);
        Assert.Equal(2, projection.Position);
    }

    [Fact]
    public async Task Activate_rejects_stale_version()
    {
        var store = EsFixture.Store();
        var projection = new VersionedCountingProjection();
        var manager = Build(store, projection);

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Open(stream, "Ann")], 0);
        await store.AppendAsync(stream, "account", [Deposit(stream, 10m)], 1);

        // Версия «построена» на чекпоинте 0, но голова уже 2.
        IVersionedProjection versioned = projection;
        await versioned.SaveCheckpointAsync("counts::v2", 0, default);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await manager.ActivateVersionAsync("counts", 2));
    }

    [Fact]
    public async Task BuildVersion_requires_versioned_projection()
    {
        var store = EsFixture.Store();
        var manager = Build(store, new PlainProjection());

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account", [Open(stream, "Ann")], 0);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await manager.BuildVersionAsync("plain", 1));
    }

    [Fact]
    public async Task Unknown_projection_name_throws()
    {
        var manager = Build(EsFixture.Store());
        await Assert.ThrowsAsync<ArgumentException>(async () => await manager.RebuildAsync("nope"));
    }

    private static EventToAppend Open(Guid stream, string holder) => new()
    {
        Payload = new AccountOpened(stream, holder, 0m),
        EventType = "contracts.account-opened",
    };

    private static EventToAppend Deposit(Guid stream, decimal amount) => new()
    {
        Payload = new MoneyDeposited(stream, amount),
        EventType = "contracts.money-deposited",
    };

    /// <summary>Проекция с per-version чекпоинтами и read-моделью (blue/green).</summary>
    private sealed class VersionedCountingProjection : Projection, IVersionedProjection
    {
        private readonly ConcurrentDictionary<string, int> _positions = new();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _counts = new();

        public override string Name => "counts";

        public int Position => _positions.GetValueOrDefault("counts", 0);

        public int Checkpoint(string name) => _positions.GetValueOrDefault(name, 0);

        public ConcurrentDictionary<string, int> Counts => _counts.GetOrAdd("counts", _ => new());

        public VersionedCountingProjection()
        {
            On<AccountOpened>((_, _) => ValueTask.CompletedTask);
            On<MoneyDeposited>((_, _) => ValueTask.CompletedTask);
        }

        public void Record(string type)
            => _counts.GetOrAdd("counts", _ => new()).AddOrUpdate(type, 1, (_, v) => v + 1);

        public override ValueTask<long> GetCheckpointAsync(CancellationToken ct)
            => ValueTask.FromResult((long)_positions.GetValueOrDefault("counts", 0));

        public override ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
            => SaveCheckpointAsync("counts", position, ct);

        public override ValueTask ResetAsync(CancellationToken ct)
            => ResetAsync("counts", ct);

        public ValueTask<long> GetCheckpointAsync(string checkpointName, CancellationToken ct)
            => ValueTask.FromResult((long)_positions.GetValueOrDefault(checkpointName, 0));

        public ValueTask SaveCheckpointAsync(string checkpointName, long position, CancellationToken ct)
        {
            _positions[checkpointName] = (int)position;
            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync(string checkpointName, CancellationToken ct)
        {
            _positions[checkpointName] = 0;
            _counts.TryRemove(checkpointName, out _);
            return ValueTask.CompletedTask;
        }

        public override ValueTask ApplyAsync(StoredEvent stored, object @event, CancellationToken ct)
        {
            _counts.GetOrAdd("counts", _ => new()).AddOrUpdate(stored.EventType, 1, (_, v) => v + 1);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PlainProjection : Projection
    {
        public override string Name => "plain";

        private long _position;

        public override ValueTask<long> GetCheckpointAsync(CancellationToken ct)
            => ValueTask.FromResult(_position);

        public override ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
        {
            _position = position;
            return ValueTask.CompletedTask;
        }

        public override ValueTask ResetAsync(CancellationToken ct)
        {
            _position = 0;
            return ValueTask.CompletedTask;
        }
    }
}
