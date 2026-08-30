using AvtoBus.InMemory;
using AvtoBus.Runtime;
using Xunit;

namespace AvtoBus.Tests;

public class DlqReaderTests : IAsyncDisposable
{
    private readonly InMemoryTransport _transport = new();
    private readonly TransportRegistry _registry;
    private readonly DlqReader _reader;

    public DlqReaderTests()
    {
        _registry = new TransportRegistry([_transport], "inmemory");
        _reader = new DlqReader(_registry);
    }

    private readonly HashSet<Guid> _deadLettered = [];

    private async Task<Guid> DropToDlqAsync(string originalQueue)
    {
        var envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "orders.charge-card.v1",
            Body = "{}"u8.ToArray(),
            SentAt = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>
            {
                [BusHeaders.OriginalDestination] = TransportDestination.Queue(originalQueue).ToString(),
                [BusHeaders.DeadLetterReason] = "исчерпаны ретраи",
                [BusHeaders.FailedAt] = DateTimeOffset.UtcNow.ToString("O"),
            },
        };

        // Падаем в error-очередь как это делает ConsumerHost.DeadLetterAsync.
        await _transport.SendAsync(envelope, TransportDestination.Queue("orders.error"));
        _deadLettered.Add(envelope.MessageId);
        return envelope.MessageId;
    }

    [Fact]
    public async Task Browse_returns_dead_lettered_messages_and_keeps_them_in_the_queue()
    {
        await DropToDlqAsync("orders");
        await DropToDlqAsync("orders");

        var messages = await _reader.BrowseAsync(TransportDestination.Queue("orders.error"));

        Assert.Equal(2, messages.Count);
        Assert.All(messages, m => Assert.NotNull(m.Reason));
        Assert.All(messages, m => Assert.Equal("queue:orders", m.OriginalDestination));

        // Просмотр не удаляет: оба сообщения остаются в DLQ.
        Assert.Equal(2, _transport.QueueDepths["orders.error"]);
    }

    [Fact]
    public async Task Replay_moves_one_message_back_to_original_queue()
    {
        var first = await DropToDlqAsync("orders");
        var second = await DropToDlqAsync("orders");

        var ok = await _reader.ReplayAsync(TransportDestination.Queue("orders.error"), first);

        Assert.True(ok);
        Assert.Equal(1, _transport.QueueDepths["orders"]);
        Assert.Equal(1, _transport.QueueDepths["orders.error"]);

        // Осталось именно второе сообщение.
        var remaining = await _reader.BrowseAsync(TransportDestination.Queue("orders.error"));
        Assert.Single(remaining);
        Assert.Equal(second, remaining[0].Envelope.MessageId);
    }

    [Fact]
    public async Task Replay_returns_false_when_message_not_found()
    {
        await DropToDlqAsync("orders");

        var ok = await _reader.ReplayAsync(TransportDestination.Queue("orders.error"), Guid.NewGuid());

        Assert.False(ok);
        Assert.Equal(1, _transport.QueueDepths["orders.error"]);
    }

    [Fact]
    public async Task Replay_all_moves_every_message_to_its_original_queue()
    {
        await DropToDlqAsync("orders-a");
        await DropToDlqAsync("orders-b");

        var replayed = await _reader.ReplayAllAsync(TransportDestination.Queue("orders.error"), maxPerSecond: 100);

        Assert.Equal(2, replayed);
        Assert.Equal(0, _transport.QueueDepths["orders.error"]);
        Assert.Equal(1, _transport.QueueDepths["orders-a"]);
        Assert.Equal(1, _transport.QueueDepths["orders-b"]);
    }

    [Fact]
    public async Task Replay_all_leaves_messages_without_original_destination()
    {
        var orphan = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "orders.charge-card.v1",
            Body = "{}"u8.ToArray(),
            SentAt = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string> { [BusHeaders.DeadLetterReason] = "без исходной очереди" },
        };

        await _transport.SendAsync(orphan, TransportDestination.Queue("orders.error"));

        var replayed = await _reader.ReplayAllAsync(TransportDestination.Queue("orders.error"), maxPerSecond: 100);

        Assert.Equal(0, replayed);
        Assert.Equal(1, _transport.QueueDepths["orders.error"]);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var id in _deadLettered)
            _ = id;

        return _transport.DisposeAsync();
    }
}
