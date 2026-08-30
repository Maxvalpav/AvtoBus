using System.Collections.Frozen;
using AvtoBus.Outbox.EfCore;
using Xunit;

namespace AvtoBus.Tests;

public class OutboxSerializerTests
{
    private static Envelope Sample() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        CausationId = Guid.NewGuid(),
        MessageType = "orders.order-placed.v1",
        Body = System.Text.Encoding.UTF8.GetBytes("{\"total\":42}"),
        ContentType = "application/json",
        SentAt = DateTimeOffset.UtcNow,
        DeliverAt = DateTimeOffset.UtcNow.AddMinutes(5),
        TimeToLive = TimeSpan.FromMinutes(30),
        PartitionKey = "order:abc",
        TenantId = "acme",
        ReplyTo = "reply:q",
        DeliveryAttempt = 3,
        TraceParent = "00-...-01",
        Headers = new Dictionary<string, string> { ["consumer"] = "default" }.ToFrozenDictionary(),
    };

    [Fact]
    public void Serialize_Deserialize_RoundTripPreservesEnvelope()
    {
        var ser = new JsonEnvelopeSerializer();
        var original = Sample();

        var blob = ser.Serialize(original);
        var copy = ser.Deserialize(blob);

        Assert.Equal(original.MessageId, copy.MessageId);
        Assert.Equal(original.CorrelationId, copy.CorrelationId);
        Assert.Equal(original.CausationId, copy.CausationId);
        Assert.Equal(original.MessageType, copy.MessageType);
        Assert.Equal(original.Body.Span.ToArray(), copy.Body.Span.ToArray());
        Assert.Equal(original.ContentType, copy.ContentType);
        Assert.Equal(original.SentAt, copy.SentAt);
        Assert.Equal(original.DeliverAt, copy.DeliverAt);
        Assert.Equal(original.TimeToLive, copy.TimeToLive);
        Assert.Equal(original.PartitionKey, copy.PartitionKey);
        Assert.Equal(original.TenantId, copy.TenantId);
        Assert.Equal(original.ReplyTo, copy.ReplyTo);
        Assert.Equal(original.DeliveryAttempt, copy.DeliveryAttempt);
        Assert.Equal(original.TraceParent, copy.TraceParent);
        Assert.Equal(original.Headers["consumer"], copy.Headers["consumer"]);
    }
}

public class OutboxSignalTests
{
    [Fact]
    public async Task Nudge_WakesWaitBeforeTimeout()
    {
        var signal = new ChannelOutboxSignal();

        var wait = signal.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        await Task.Delay(50);
        signal.Nudge();

        await wait.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Wait_ReturnsAfterTimeoutWithoutNudge()
    {
        var signal = new ChannelOutboxSignal();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        await signal.WaitAsync(TimeSpan.FromMilliseconds(100), CancellationToken.None);

        stopwatch.Stop();
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(80));
    }
}
