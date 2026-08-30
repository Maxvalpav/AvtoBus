using System.Threading.Channels;
using AvtoBus;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>
/// Transport Conformance-kit (идея 98, док 18 §7): каждый транспорт обязан пройти этот сьют,
/// работающий только через публичный контракт <see cref="ITransport"/>. Новый транспорт —
/// не риск, а понятный чек-лист: наследуешь класс и реализуешь <see cref="CreateAsync"/>.
/// </summary>
/// <remarks>
/// Сьюит намеренно не использует шину и харнесс: проверяется сам транспорт, а не пайплайн.
/// Внешние брокеры (RabbitMQ/Kafka) подключаются реальной инфраструктурой в CI.
/// </remarks>
public abstract class TransportConformanceTests
{
    protected abstract Task<ITransport> CreateAsync();

    protected virtual TimeSpan Timeout { get; } = TimeSpan.FromSeconds(10);

    private static Envelope Make(string type = "conformance.test.v1") => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = type,
        Body = System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")),
        SentAt = DateTimeOffset.UtcNow,
    };

    private static TransportDestination UniqueQueue(string prefix)
        => TransportDestination.Queue($"{prefix}.{Guid.NewGuid():N}");

    [Fact]
    public async Task Send_then_Receive_delivers_same_envelope()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("send-recv");
        var envelope = Make();

        await transport.SendAsync(envelope, queue);

        await using var consumer = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await consumer.NextAsync(Timeout);

        Assert.NotNull(received);
        Assert.Equal(envelope.MessageId, received.Envelope.MessageId);
        Assert.Equal(envelope.MessageType, received.Envelope.MessageType);
        Assert.Equal(envelope.Body.ToArray(), received.Envelope.Body.ToArray());
    }

    [Fact]
    public async Task Headers_are_preserved_end_to_end()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("headers");
        var envelope = Make().WithHeader("x-test", "42").WithHeader("x-other", "∅");

        await transport.SendAsync(envelope, queue);

        await using var consumer = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await consumer.NextAsync(Timeout);

        Assert.NotNull(received);
        Assert.Equal("42", received.Envelope.Header("x-test"));
        Assert.Equal("∅", received.Envelope.Header("x-other"));
    }

    [Fact]
    public async Task Message_survives_until_receiving_consumer_exists()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("late-consumer");
        var envelope = Make();

        // Отправка до подъёма консьюмера не должна теряться (идея 55).
        await transport.SendAsync(envelope, queue);

        await using var consumer = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await consumer.NextAsync(Timeout);

        Assert.NotNull(received);
        Assert.Equal(envelope.MessageId, received.Envelope.MessageId);
    }

    [Fact]
    public async Task Topic_is_replicated_to_each_consumer_group()
    {
        await using var transport = await CreateAsync();
        var topic = TransportDestination.Topic($"orders.{Guid.NewGuid():N}.placed");

        // Подписки через топик: каждая группа получает копию сообщения.
        await using var groupA = new Consumer(transport, new TransportSubscription(topic, "group-a"));
        await using var groupB = new Consumer(transport, new TransportSubscription(topic, "group-b"));

        await transport.SendAsync(Make(), topic);

        var gotA = await groupA.NextAsync(Timeout);
        var gotB = await groupB.NextAsync(Timeout);

        Assert.NotNull(gotA);
        Assert.NotNull(gotB);
    }

    [Fact]
    public async Task Queue_messages_are_shared_between_consumers_of_a_group()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("shared");
        const int total = 10;

        // Два консьюмера одной группы делят сообщения очереди: каждое уходит ровно одному.
        await using var consumerA = new Consumer(transport, new TransportSubscription(queue, "group"));
        await using var consumerB = new Consumer(transport, new TransportSubscription(queue, "group"));

        for (var i = 0; i < total; i++)
            await transport.SendAsync(Make(), queue);

        var received = 0;
        var deadline = DateTime.UtcNow + Timeout;

        while (received < total && DateTime.UtcNow < deadline)
        {
            var a = await consumerA.NeedNextOrNullAsync(TimeSpan.FromMilliseconds(20));
            var b = await consumerB.NeedNextOrNullAsync(TimeSpan.FromMilliseconds(20));
            received += (a is not null ? 1 : 0) + (b is not null ? 1 : 0);
            await Task.Delay(10);
        }

        Assert.Equal(total, received);
    }

    [Fact]
    public async Task Acknowledge_prevents_redelivery()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("ack");
        var envelope = Make();

        await transport.SendAsync(envelope, queue);

        await using var first = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await first.NextAsync(Timeout);
        Assert.NotNull(received);
        await received.AcknowledgeAsync();

        // Подтверждённое сообщение не передоставляется новому консьюмеру.
        await using var second = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        Assert.Null(await second.NeedNextOrNullAsync(TimeSpan.FromMilliseconds(200)));
    }

    [Fact]
    public async Task Reject_without_requeue_removes_from_delivery()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("reject");

        await transport.SendAsync(Make(), queue);

        await using var first = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await first.NextAsync(Timeout);
        Assert.NotNull(received);
        await received.RejectAsync(requeue: false);

        await using var second = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        Assert.Null(await second.NeedNextOrNullAsync(TimeSpan.FromMilliseconds(200)));
    }

    [Fact]
    public async Task Delivery_attempt_increments_on_redelivery()
    {
        await using var transport = await CreateAsync();
        var queue = UniqueQueue("attempt");
        var envelope = Make();

        await transport.SendAsync(envelope, queue);

        await using var first = new Consumer(transport, new TransportSubscription(queue, "conformance"));
        var received = await first.NextAsync(Timeout);
        Assert.NotNull(received);
        await received.RejectAsync(requeue: true);

        var again = await first.NextAsync(Timeout);
        Assert.NotNull(again);
        Assert.True(again.Envelope.DeliveryAttempt > received.Envelope.DeliveryAttempt);
    }

    /// <summary>
    /// Фоновый потребитель подписки. Конструктор запускает первый MoveNext — момент,
    /// когда транспорт выполняет привязку очереди/группы, — не блокируя тест.
    /// Сообщения складываются в канал и читаются по требованию.
    /// </summary>
    private sealed class Consumer : IAsyncDisposable
    {
        private readonly IAsyncEnumerator<ITransportMessage> _enumerator;
        private readonly Channel<ITransportMessage> _channel = Channel.CreateUnbounded<ITransportMessage>();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _feed;

        public Consumer(ITransport transport, TransportSubscription subscription)
        {
            _enumerator = transport.ReceiveAsync(subscription, _cts.Token).GetAsyncEnumerator();
            _feed = FeedAsync();
        }

        private async Task FeedAsync()
        {
            try
            {
                while (await _enumerator.MoveNextAsync())
                {
                    await _channel.Writer.WriteAsync(_enumerator.Current, _cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _channel.Writer.TryComplete();
            }
        }

        public async Task<ITransportMessage?> NeedNextOrNullAsync(TimeSpan timeout)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            cts.CancelAfter(timeout);

            try
            {
                return await _channel.Reader.ReadAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        public async Task<ITransportMessage?> NextAsync(TimeSpan timeout) => await NeedNextOrNullAsync(timeout);

        public async ValueTask DisposeAsync()
        {
            await _cts.CancelAsync();
            try { await _feed; }
            catch (OperationCanceledException) { }
            await _enumerator.DisposeAsync();
            _cts.Dispose();
        }
    }
}
