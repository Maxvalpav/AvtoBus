using System.Text;
using AvtoBus.InMemory;
using AvtoBus.Kafka;
using AvtoBus.Local;
using AvtoBus.Nats;
using AvtoBus.RabbitMq;
using AvtoBus.Redis;
using AvtoBus.Runtime;
using AvtoBus.Scheduling;
using AvtoBus.Sql;
using Microsoft.Extensions.Logging.Abstractions;

namespace AvtoBus.Tests;

/// <summary>
/// Единая отложенная доставка (аудит 04 §1.1): capability-маркер нативной задержки,
/// маппинг адреса ретрая для Kafka/NATS/Redis, fallback в schedule-стор.
/// </summary>
public class DelayedDeliveryTests
{
    [Theory]
    [InlineData(typeof(InMemoryTransport), true)]
    [InlineData(typeof(LocalQueueTransport), true)]
    [InlineData(typeof(RabbitMqTransport), true)]
    [InlineData(typeof(SqlTransport), true)]
    [InlineData(typeof(AvtoBus.AzureServiceBus.AsbTransport), true)]
    [InlineData(typeof(KafkaTransport), false)]
    [InlineData(typeof(NatsTransport), false)]
    [InlineData(typeof(RedisTransport), false)]
    public void Native_delay_capability_matches_matrix(Type transport, bool expected)
    {
        // Через тип, а не инстанс: конструкторы внешних транспортов коннектятся к брокерам.
        Assert.Equal(expected, transport.IsAssignableTo(typeof(ISupportsDelayedDelivery)));
    }

    [Fact]
    public void Kafka_and_redis_retry_to_source()
    {
        var source = TransportDestination.Queue("orders");
        var sub = new TransportSubscription(TransportDestination.Topic("orders"), "g");

        Assert.Equal(source, KafkaTransport.MapRetryDestination(source, sub));
        Assert.Equal(source, RedisTransport.MapRetryDestination(source, sub));
    }

    [Fact]
    public void Nats_retries_to_subscription_not_group_queue()
    {
        // Синтетический Source Queue("orders:g") не читает никто; сабджекты не
        // зависят от Kind — редиливери в исходную подписку доходит до групп.
        var synthetic = TransportDestination.Queue("orders:g");
        var original = TransportDestination.Topic("orders");
        var sub = new TransportSubscription(original, "g");

        Assert.Equal(original, NatsTransport.MapRetryDestination(synthetic, sub));
    }

    [Fact]
    public async Task Fallback_persists_retry_to_schedule_store()
    {
        var store = new InMemoryScheduleStore();
        var fallback = new DelayedDeliveryFallback(
            store, new StubEnvelopeFactory(), TimeProvider.System, NullLogger<DelayedDeliveryFallback>.Instance);

        var envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "orders.order-placed",
            Body = Encoding.UTF8.GetBytes("{}"),
            SentAt = DateTimeOffset.UtcNow,
        };

        Assert.True(await fallback.TryDeferAsync(
            envelope, TransportDestination.Queue("orders"), "kafka", TimeSpan.FromMinutes(5)));

        // До срока — тихо, после — клеймится с полями для редиливери.
        Assert.Empty(await store.ClaimDueAsync(DateTime.UtcNow, 10, "test"));
        var due = await store.ClaimDueAsync(DateTime.UtcNow.AddHours(1), 10, "test");
        var single = Assert.Single(due);
        Assert.Equal("orders", single.Destination);
        Assert.Equal("kafka", single.Transport);
        Assert.Equal("orders.order-placed", single.MessageType);
        Assert.True(single.DeliverAt - single.CreatedAt >= TimeSpan.FromMinutes(4));
    }

    [Fact]
    public async Task Fallback_rejects_non_positive_delay_and_store_errors()
    {
        var store = new InMemoryScheduleStore();
        var fallback = new DelayedDeliveryFallback(
            store, new StubEnvelopeFactory(), TimeProvider.System, NullLogger<DelayedDeliveryFallback>.Instance);

        var envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "t",
            Body = ReadOnlyMemory<byte>.Empty,
            SentAt = DateTimeOffset.UtcNow,
        };

        Assert.False(await fallback.TryDeferAsync(envelope, TransportDestination.Queue("q"), "kafka", TimeSpan.Zero));

        var failing = new DelayedDeliveryFallback(
            new ThrowingStore(), new StubEnvelopeFactory(), TimeProvider.System, NullLogger<DelayedDeliveryFallback>.Instance);
        Assert.False(await failing.TryDeferAsync(envelope, TransportDestination.Queue("q"), "kafka", TimeSpan.FromMinutes(1)));
    }

    private sealed class StubEnvelopeFactory : IEnvelopeFactory
    {
        public Envelope Create(object message, Type messageType, MessageOptions? options, Envelope? parent)
            => throw new NotSupportedException();

        public byte[] Serialize(Envelope envelope)
            => Encoding.UTF8.GetBytes($"{envelope.MessageId:N}|{envelope.MessageType}");

        public Envelope Deserialize(ReadOnlyMemory<byte> blob)
        {
            var parts = Encoding.UTF8.GetString(blob.Span).Split('|');
            return new Envelope
            {
                MessageId = Guid.Parse(parts[0]),
                MessageType = parts[1],
                Body = ReadOnlyMemory<byte>.Empty,
                SentAt = DateTimeOffset.UtcNow,
            };
        }
    }

    private sealed class ThrowingStore : IScheduleStore
    {
        public ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default)
            => ValueTask.FromException<Guid>(new InvalidOperationException("store down"));
        public ValueTask CancelAsync(Guid token, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(DateTime now, int batchSize, string claimedBy, CancellationToken ct = default)
            => ValueTask.FromResult<IReadOnlyList<ScheduledMessage>>([]);
        public ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(DateTime now, string claimedBy, CancellationToken ct = default)
            => ValueTask.FromResult<IReadOnlyList<CronSchedule>>([]);
        public ValueTask UpdateCronAfterFireAsync(long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default)
            => ValueTask.FromResult<IReadOnlyList<CronSchedule>>([]);
        public ValueTask DeleteCronAsync(long id, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
