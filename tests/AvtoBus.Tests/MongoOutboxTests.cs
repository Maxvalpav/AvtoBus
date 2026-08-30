using AvtoBus.Mongo;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class MongoOutboxTests
{
    [Fact]
    public async Task InMemory_mongo_store_add_read_mark()
    {
        var store = new InMemoryMongoOutboxStore();
        var doc = new MongoOutboxDoc(Guid.NewGuid(), "a", new byte[] { 1, 2 }, "q", "{}", DateTimeOffset.UtcNow, false);
        await store.AddAsync(doc, CancellationToken.None);
        var pending = new List<MongoOutboxDoc>();
        await foreach (var d in store.ReadPendingAsync(CancellationToken.None)) pending.Add(d);
        Assert.Single(pending);
        await store.MarkDispatchedAsync(doc.Id, CancellationToken.None);
        pending.Clear();
        await foreach (var d in store.ReadPendingAsync(CancellationToken.None)) pending.Add(d);
        Assert.Empty(pending);
    }

    [Fact]
    public async Task Mongo_relay_marks_dispatched_after_read()
    {
        var store = new InMemoryMongoOutboxStore();
        var opts = new MongoOutboxOptions { RelayInterval = TimeSpan.FromMilliseconds(20) };
        var relay = new MongoOutboxRelay(store, opts, new ServiceCollection().BuildServiceProvider());
        await relay.StartAsync(CancellationToken.None);
        var doc = new MongoOutboxDoc(Guid.NewGuid(), "a", new byte[] { 1 }, "q", "{}", DateTimeOffset.UtcNow, false);
        await store.AddAsync(doc, CancellationToken.None);
        await Task.Delay(100);
        var pending = new List<MongoOutboxDoc>();
        await foreach (var d in store.ReadPendingAsync(CancellationToken.None)) pending.Add(d);
        Assert.Empty(pending);
        await relay.StopAsync(CancellationToken.None);
    }
}
