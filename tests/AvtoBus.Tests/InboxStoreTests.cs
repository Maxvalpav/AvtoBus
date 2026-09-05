using AvtoBus.Configuration;
using AvtoBus.Redis;
using AvtoBus.Runtime;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Inbox-стор без реляционной БД (аудит 04 §1.2): in-memory LRU для монолита,
/// Redis (SET NX EX) для мульти-инстанс. Транзакционный EF-inbox не затрагивается.
/// </summary>
public class InboxStoreTests
{
    [Fact]
    public async Task InMemory_first_true_duplicate_false()
    {
        var store = new InMemoryInboxStore(TimeSpan.FromHours(1), new FakeTimeProvider());
        var id = Guid.NewGuid();

        Assert.True(await store.TryMarkProcessedAsync(id, "q"));
        Assert.False(await store.TryMarkProcessedAsync(id, "q"));
        // Другой консьюмер — не дубликат.
        Assert.True(await store.TryMarkProcessedAsync(id, "other-q"));
    }

    [Fact]
    public async Task InMemory_forget_releases()
    {
        var store = new InMemoryInboxStore(TimeSpan.FromHours(1), new FakeTimeProvider());
        var id = Guid.NewGuid();

        Assert.True(await store.TryMarkProcessedAsync(id, "q"));
        store.Forget(id, "q");
        Assert.True(await store.TryMarkProcessedAsync(id, "q"));
    }

    [Fact]
    public async Task InMemory_window_expiry_allows_reprocess()
    {
        // Локальный тестовый FakeTimeProvider: только часы, без таймеров —
        // здесь таймеры и не нужны, стор читает только GetUtcNow.
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero));
        var store = new InMemoryInboxStore(TimeSpan.FromHours(1), time);
        var id = Guid.NewGuid();

        Assert.True(await store.TryMarkProcessedAsync(id, "q"));
        time.Advance(TimeSpan.FromMinutes(61));
        Assert.True(await store.TryMarkProcessedAsync(id, "q"));
    }

    [Fact]
    public async Task Pipeline_marks_through_configured_store()
    {
        var seen = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .UseInMemoryInbox(TimeSpan.FromHours(1))
                .Subscribe<PlaceOrder>(ctx =>
                {
                    seen.TrySetResult(ctx.Envelope.MessageId);
                    return Task.CompletedTask;
                }));

        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "cust-1", 10m));
        var messageId = await seen.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Процессор пометил сообщение через стор из DI: повтор той же пары — дубликат.
        var store = harness.Services.GetRequiredService<IInboxStore>();
        Assert.IsType<InMemoryInboxStore>(store);
        var consumer = RoutingTable.CommandQueueName(typeof(PlaceOrder));
        Assert.False(await store.TryMarkProcessedAsync(messageId, consumer));
    }

    [Fact]
    public async Task Configured_store_wins_over_options_window()
    {
        var probe = new ProbeStore();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .UseInboxDeduplication(TimeSpan.FromHours(1))
                .UseInboxStore(probe)
                .Subscribe<PlaceOrder>(_ => Task.CompletedTask));

        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "cust-1", 10m));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (probe.Marks == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(25);
        Assert.Equal(1, probe.Marks);
    }

    private sealed class ProbeStore : IInboxStore
    {
        private int _marks;
        public int Marks => _marks;

        public ValueTask<bool> TryMarkProcessedAsync(Guid messageId, string consumer, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _marks);
            return ValueTask.FromResult(true);
        }

        public void Forget(Guid messageId, string consumer) { }
    }

    private const string RedisEnv = "AVTOBUS_REDIS_URL";

    [Fact]
    public void Redis_rejects_non_positive_window_before_connect()
    {
        // Без сервера: валидация окна идёт до ConnectionMultiplexer.Connect.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RedisInboxStore(new RedisOptions { Configuration = "localhost:6379" }, TimeSpan.Zero));
    }

    [Fact]
    public void Redis_key_format_is_stable()
    {
        var id = Guid.Parse("11111111-2222-3333-4444-555555555555");
        Assert.Equal(
            "avtobus:inbox:orders:11111111222233334444555555555555",
            RedisInboxStore.Key(id, "orders"));
    }

    [Fact]
    public async Task Redis_set_nx_ex_dedups_and_forgets()
    {
        var url = Environment.GetEnvironmentVariable(RedisEnv);
        if (url is null)
            Assert.Skip($"Redis-сервер недоступен: задайте {RedisEnv}");

        using var store = new RedisInboxStore(new RedisOptions { Configuration = url }, TimeSpan.FromMinutes(30));
        var id = Guid.NewGuid();
        const string consumer = "inbox-store-test";

        Assert.True(await store.TryMarkProcessedAsync(id, consumer));
        Assert.False(await store.TryMarkProcessedAsync(id, consumer));
        store.Forget(id, consumer);
        Assert.True(await store.TryMarkProcessedAsync(id, consumer));
        store.Forget(id, consumer);
    }
}
