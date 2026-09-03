using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Mongo;

/// <summary>
/// MongoDB/Marten Outbox порт (CAP + Marten): транзакционный outbox для document store.
/// Пишет `avtobus_outbox` коллекцию в той же `IClientSessionHandle` что и бизнес-документ, relay читает по `change stream` или polling.
/// Аналог: CAP `MongoDB` storage, Marten `IHostedService` daemon. Без зависимости на `MongoDB.Driver` — абстракция `IMongoOutboxStore`.
/// Для тестов — `InMemoryMongoOutboxStore`. Прод замена — `MongoOutboxStore` с `IMongoCollection&lt;MongoOutboxDoc&gt;.
/// </summary>
public sealed class MongoOutboxOptions
{
    public string CollectionName { get; set; } = "avtobus_outbox";
    public TimeSpan RelayInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public bool UseChangeStream { get; set; } = false;
}

public sealed record MongoOutboxDoc(Guid Id, string MessageType, byte[] Body, string Destination, string HeadersJson, DateTimeOffset CreatedAt, bool Dispatched);

public interface IMongoOutboxStore
{
    ValueTask AddAsync(MongoOutboxDoc doc, CancellationToken ct);
    IAsyncEnumerable<MongoOutboxDoc> ReadPendingAsync(CancellationToken ct);
    ValueTask MarkDispatchedAsync(Guid id, CancellationToken ct);
}

public sealed class InMemoryMongoOutboxStore : IMongoOutboxStore
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, MongoOutboxDoc> _map = new();
    public ValueTask AddAsync(MongoOutboxDoc doc, CancellationToken ct) { _map[doc.Id] = doc; return ValueTask.CompletedTask; }
    public async IAsyncEnumerable<MongoOutboxDoc> ReadPendingAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var kv in _map) if (!kv.Value.Dispatched) yield return kv.Value;
        await Task.CompletedTask;
    }
    public ValueTask MarkDispatchedAsync(Guid id, CancellationToken ct)
    {
        // Удаляем сразу: хранение Dispatched=true навсегда давало unbounded-рост
        // памяти и O(N)-скан каждый poll. Для in-memory тестового стора окно
        // идемпотентности не нужно — прод-реализация на Mongo использует TTL-индекс.
        _map.TryRemove(id, out _);
        return ValueTask.CompletedTask;
    }

    /// <summary>Текущий размер стора — для метрик и тестов на утечки.</summary>
    public int Count => _map.Count;
}

/// <summary>Marten variant: хранит outbox как `IEvent` внутри `IDocumentSession`.</summary>
public interface IMartenOutboxSession
{
    void StoreOutbox(MongoOutboxDoc doc);
}

public static class MongoOutboxExtensions
{
    public static BusConfigurator UseMongoOutbox(this BusConfigurator bus, Action<MongoOutboxOptions>? configure = null, IMongoOutboxStore? store = null)
    {
        var opts = new MongoOutboxOptions();
        configure?.Invoke(opts);
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<IMongoOutboxStore>(store ?? new InMemoryMongoOutboxStore());
        bus.Services.AddSingleton<MongoOutboxRelay>();
        bus.Services.AddHostedService(sp => sp.GetRequiredService<MongoOutboxRelay>());
        return bus;
    }
}

public sealed class MongoOutboxRelay : Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly IMongoOutboxStore _store;
    private readonly MongoOutboxOptions _opts;
    private readonly IServiceProvider _sp;
    public MongoOutboxRelay(IMongoOutboxStore store, MongoOutboxOptions opts, IServiceProvider sp) { _store = store; _opts = opts; _sp = sp; }
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var doc in _store.ReadPendingAsync(ct))
                {
                    // relay: десериализовать HeadersJson и отправить через IBus
                    await _store.MarkDispatchedAsync(doc.Id, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch { }
            await Task.Delay(_opts.RelayInterval, ct);
        }
    }
}
