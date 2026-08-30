using System.Collections.Concurrent;
using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Projections;
using AvtoBus.Tests.EventSourcing.Contracts;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

/// <summary>Проекция-счётчик: сколько событий каждого типа уже применено.</summary>
public sealed class CountingProjection : Projection
{
    public override string Name => "counts";

    public ConcurrentDictionary<string, int> Counts { get; } = new();

    public CountingProjection()
    {
        On<AccountOpened>((_, _) => ValueTask.CompletedTask);
        On<MoneyDeposited>((_, _) => ValueTask.CompletedTask);
    }

    public override ValueTask<long> GetCheckpointAsync(CancellationToken ct) => ValueTask.FromResult(_checkpoint);

    public override ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
    {
        _checkpoint = position;
        Interlocked.Increment(ref _saves);
        return ValueTask.CompletedTask;
    }

    public override ValueTask ResetAsync(CancellationToken ct)
    {
        _checkpoint = 0;
        Counts.Clear();
        return ValueTask.CompletedTask;
    }

    private long _checkpoint;
    private long _saves;

    public long SaveCount => _saves;

    public void Record(string type) => Counts.AddOrUpdate(type, 1, (_, v) => v + 1);
}

public class ProjectionTests
{
    [Fact]
    public async Task ApplyAsync_dispatches_only_handled_types()
    {
        var projection = new CountingProjection();

        await projection.ApplyAsync(
            NewStored("contracts.account-opened", 1), new AccountOpened(Guid.NewGuid(), "Ann", 0m), default);
        await projection.ApplyAsync(
            NewStored("contracts.money-deposited", 2), new MoneyDeposited(Guid.NewGuid(), 5m), default);

        // HandledEventTypes собраны из On<>().
        Assert.Equal(new[] { "contracts.account-opened", "contracts.money-deposited" }, projection.HandledEventTypes);
    }

    [Fact]
    public async Task Daemon_progresses_checkpoint_eventually_to_head()
    {
        var store = EsFixture.Store();
        var projection = new SyncProjection();
        var daemon = BuildDaemon(store, projection);

        // 3 события, daemon должен применить все и остановиться на headSequence.
        var s1 = Guid.NewGuid();
        await store.AppendAsync(s1, "account", [Event(s1, "open")], 0);
        await store.AppendAsync(s1, "account", [Event(s1, "deposit")], 1);
        await store.AppendAsync(Guid.NewGuid(), "account", [Event(Guid.NewGuid(), "open")], 0);

        await daemon.StartAsync(CancellationToken.None);
        try
        {
            // Ждём, пока daemon дочитает до головы (3 события) и офснется на чекпоинт.
            await TaskHelper.WaitUntilAsync(() => projection.Snapshot() == 3, TimeSpan.FromSeconds(5));
        }
        finally
        {
            await daemon.StopAsync(CancellationToken.None);
        }

        Assert.Equal(3, projection.Snapshot());
    }

    // Проекция, хранящая чекпоинт и состояние в памяти (быстрее, чем EF).
    private sealed class SyncProjection : Projection
    {
        public override string Name => "sync";
        public int _count;
        public long _cp;

        public int Snapshot() => _count;

        public override ValueTask<long> GetCheckpointAsync(CancellationToken ct) => ValueTask.FromResult(_cp);

        public override ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
        {
            _cp = position;
            return ValueTask.CompletedTask;
        }

        public override ValueTask ResetAsync(CancellationToken ct)
        {
            _cp = 0;
            _count = 0;
            return ValueTask.CompletedTask;
        }

        public override ValueTask ApplyAsync(StoredEvent stored, object @event, CancellationToken ct)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }

    private static ProjectionDaemon BuildDaemon(IEventStore store, IProjection projection)
        => new(
            store,
            new[] { projection },
            EsFixture.Serializer(),
            new UpcasterChain([]),
            new ProjectionDaemonOptions { BatchSize = 100, IdleDelay = TimeSpan.FromMilliseconds(20) },
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ProjectionDaemon>.Instance);

    private static EventToAppend Event(Guid id, string kind) => new()
    {
        Payload = kind == "open" ? new AccountOpened(id, "Ann", 0m) : new MoneyDeposited(id, 10m),
        EventType = kind == "open" ? "contracts.account-opened" : "contracts.money-deposited",
    };

    private static StoredEvent NewStored(string type, long seq = 1) => new()
    {
        GlobalSequence = seq,
        StreamId = Guid.NewGuid(),
        StreamType = "account",
        Version = (int)seq,
        EventType = type,
        SchemaVersion = 1,
        Data = Array.Empty<byte>(),
        Timestamp = DateTimeOffset.UtcNow,
    };
}

internal static class TaskHelper
{
    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition not met within timeout");
            await Task.Delay(15);
        }
    }
}
