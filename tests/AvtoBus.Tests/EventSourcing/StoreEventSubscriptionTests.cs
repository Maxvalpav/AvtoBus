using System.Collections.Concurrent;
using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Projections;
using AvtoBus.Tests.EventSourcing.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AvtoBus.Tests.EventSourcing;

public class StoreEventSubscriptionTests
{
    private sealed class RecordingBus : IBus
    {
        public ConcurrentQueue<(object Message, PublishOptions? Options)> Published { get; } = new();

        public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default)
            where T : class
        {
            Published.Enqueue((@event, options));
            return ValueTask.CompletedTask;
        }

        public ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default)
            where T : class
            => throw new NotSupportedException();

        public ValueTask<TReply> RequestAsync<TRequest, TReply>(
            TRequest request, TimeSpan? timeout = null, CancellationToken ct = default)
            where TRequest : class where TReply : class
            => throw new NotSupportedException();

        public ValueTask<ScheduledToken> ScheduleAsync<T>(T message, DateTimeOffset at, CancellationToken ct = default)
            where T : class
            => throw new NotSupportedException();

        public ValueTask CancelScheduledAsync(ScheduledToken token, CancellationToken ct = default)
            => throw new NotSupportedException();

        public ValueTask EnqueueLocal<T>(T message, string? queueName = null, CancellationToken ct = default)
            where T : class
            => throw new NotSupportedException();
    }

    [Fact]
    public async Task Subscription_publishes_store_events_to_bus()
    {
        var store = EsFixture.Store();
        var bus = new RecordingBus();

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account",
            [new EventToAppend { Payload = new AccountOpened(stream, "Ann", 100m), EventType = "contracts.account-opened" }], 0);

        using var subscription = new StoreEventSubscription(
            store,
            EsFixture.Serializer(),
            new UpcasterChain([]),
            bus,
            new StoreSubscriptionOptions { Name = "test", StreamType = "account" },
            NullLogger<StoreEventSubscription>.Instance);

        await subscription.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntil(() => bus.Published.Count >= 1, TimeSpan.FromSeconds(5));

            var (message, options) = Assert.Single(bus.Published);
            var opened = Assert.IsType<AccountOpened>(message);
            Assert.Equal(stream, opened.AccountId);
            Assert.Equal(stream.ToString(), options!.PartitionKey);
        }
        finally
        {
            await subscription.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Subscription_honors_stream_type_filter()
    {
        var store = EsFixture.Store();
        var bus = new RecordingBus();

        var account = Guid.NewGuid();
        await store.AppendAsync(account, "account",
            [new EventToAppend { Payload = new AccountOpened(account, "Ann", 100m), EventType = "contracts.account-opened" }], 0);

        using var subscription = new StoreEventSubscription(
            store,
            EsFixture.Serializer(),
            new UpcasterChain([]),
            bus,
            new StoreSubscriptionOptions { Name = "other", StreamType = "other" },
            NullLogger<StoreEventSubscription>.Instance);

        await subscription.StartAsync(CancellationToken.None);
        try
        {
            // Фильтр "other" не подхватывает события стрима "account".
            await Task.Delay(300);
            Assert.Empty(bus.Published);
        }
        finally
        {
            await subscription.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Subscription_continues_from_position()
    {
        var store = EsFixture.Store();
        var bus = new RecordingBus();

        var stream = Guid.NewGuid();
        await store.AppendAsync(stream, "account",
            [new EventToAppend { Payload = new AccountOpened(stream, "Ann", 100m), EventType = "contracts.account-opened" }], 0);
        await store.AppendAsync(stream, "account",
            [new EventToAppend { Payload = new MoneyDeposited(stream, 10m), EventType = "contracts.money-deposited" }], 1);

        // Подписка с позиции 1 — пропускает первое событие.
        using var subscription = new StoreEventSubscription(
            store,
            EsFixture.Serializer(),
            new UpcasterChain([]),
            bus,
            new StoreSubscriptionOptions { Name = "from-1", FromSequence = 1 },
            NullLogger<StoreEventSubscription>.Instance);

        await subscription.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntil(() => bus.Published.Count >= 1, TimeSpan.FromSeconds(5));

            var (message, _) = Assert.Single(bus.Published);
            Assert.IsType<MoneyDeposited>(message);
        }
        finally
        {
            await subscription.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
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
