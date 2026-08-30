using AvtoBus.Nats;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace AvtoBus.Tests;

/// <summary>
/// Юнит-тесты маппинга Envelope ↔ NATS (заголовки + body) без сервера.
/// </summary>
public sealed class NatsEnvelopeSerializerTests
{
    private static Envelope Make() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        CausationId = Guid.NewGuid(),
        MessageType = "order.placed.v1",
        Body = new byte[] { 0x01, 0x02, 0x03 },
        ContentType = "application/json",
        SentAt = DateTimeOffset.Parse("2026-08-16T10:00:00+00:00"),
        DeliverAt = DateTimeOffset.Parse("2026-08-16T10:05:00+00:00"),
        TimeToLive = TimeSpan.FromMinutes(30),
        PartitionKey = "tenant-42",
        TenantId = "tenant-42",
        ReplyTo = "orders.replies",
        DeliveryAttempt = 3,
        TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Headers = new Dictionary<string, string> { ["x-test"] = "42", ["x-other"] = "∅" },
    };

    private static NatsJSMsg<byte[]> ToJsMsg((NATS.Client.Core.NatsHeaders Headers, byte[] Body) msg)
        => new(
            new NatsMsg<byte[]>(
                subject: "orders.placed.v1",
                replyTo: null,
                size: msg.Body.Length,
                headers: msg.Headers,
                data: msg.Body,
                connection: null,
                flags: default),
            context: null!);

    [Fact]
    public void RoundTrip_preserves_envelope_metadata_and_body()
    {
        var envelope = Make();

        var restored = NatsEnvelopeSerializer.FromNats(ToJsMsg(NatsEnvelopeSerializer.ToNats(envelope)));

        Assert.Equal(envelope.MessageId, restored.MessageId);
        Assert.Equal(envelope.CorrelationId, restored.CorrelationId);
        Assert.Equal(envelope.CausationId, restored.CausationId);
        Assert.Equal(envelope.MessageType, restored.MessageType);
        Assert.Equal(envelope.Body, restored.Body);
        Assert.Equal(envelope.ContentType, restored.ContentType);
        Assert.Equal(envelope.SentAt, restored.SentAt);
        Assert.Equal(envelope.DeliverAt, restored.DeliverAt);
        Assert.Equal(envelope.TimeToLive, restored.TimeToLive);
        Assert.Equal(envelope.PartitionKey, restored.PartitionKey);
        Assert.Equal(envelope.TenantId, restored.TenantId);
        Assert.Equal(envelope.ReplyTo, restored.ReplyTo);
        Assert.Equal(envelope.TraceParent, restored.TraceParent);
    }

    [Fact]
    public void DeliveryAttempt_falls_back_to_header_when_metadata_unavailable()
    {
        var envelope = Make();
        envelope = envelope with { DeliveryAttempt = 3 };

        // Metadata заполняется клиентом только при реальной доставке; в юнит-тесте
        // DeliveryAttempt приходит из заголовка (prefers-metadata покроет conformance).
        var restored = NatsEnvelopeSerializer.FromNats(ToJsMsg(NatsEnvelopeSerializer.ToNats(envelope)));

        Assert.Equal(3, restored.DeliveryAttempt);
    }

    [Fact]
    public void RoundTrip_preserves_user_headers()
    {
        var envelope = Make().WithHeader("x-test", "42").WithHeader("x-other", "∅");

        var restored = NatsEnvelopeSerializer.FromNats(ToJsMsg(NatsEnvelopeSerializer.ToNats(envelope)));

        Assert.Equal("42", restored.Header("x-test"));
        Assert.Equal("∅", restored.Header("x-other"));
    }

    [Fact]
    public void Body_becomes_data()
    {
        var envelope = Make();

        var (_, body) = NatsEnvelopeSerializer.ToNats(envelope);

        Assert.Equal(envelope.Body, body);
    }

    [Fact]
    public void Missing_message_id_throws()
    {
        var envelope = Make();

        var (headers, body) = NatsEnvelopeSerializer.ToNats(envelope);
        headers.Remove("avtobus-message-id");

        Assert.Throws<InvalidDataException>(() =>
            NatsEnvelopeSerializer.FromNats(ToJsMsg((headers, body))));
    }
}
