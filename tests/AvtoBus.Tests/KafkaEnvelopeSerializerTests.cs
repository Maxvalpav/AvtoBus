using AvtoBus.Kafka;
using Confluent.Kafka;

namespace AvtoBus.Tests;

/// <summary>
/// Юнит-тесты маппинга Envelope ↔ Kafka (заголовки + value) без брокера.
/// Гарантируют, что служебные заголовки, пользовательские заголовки и тело
/// переживают round-trip.
/// </summary>
public sealed class KafkaEnvelopeSerializerTests
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

    private static ConsumeResult<string, byte[]> ToConsumeResult(Message<string, byte[]> message)
        => new()
        {
            Message = message,
            Partition = 0,
            Offset = 42,
            Topic = "orders.placed.v1",
        };

    [Fact]
    public void RoundTrip_preserves_envelope_metadata_and_body()
    {
        var envelope = Make();

        var kafka = KafkaEnvelopeSerializer.ToKafka(envelope);
        var restored = KafkaEnvelopeSerializer.FromKafka(ToConsumeResult(kafka));

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
        Assert.Equal(envelope.DeliveryAttempt, restored.DeliveryAttempt);
        Assert.Equal(envelope.TraceParent, restored.TraceParent);
    }

    [Fact]
    public void RoundTrip_preserves_user_headers()
    {
        var envelope = Make().WithHeader("x-test", "42").WithHeader("x-other", "∅");

        var restored = KafkaEnvelopeSerializer.FromKafka(
            ToConsumeResult(KafkaEnvelopeSerializer.ToKafka(envelope)));

        Assert.Equal("42", restored.Header("x-test"));
        Assert.Equal("∅", restored.Header("x-other"));
    }

    [Fact]
    public void PartitionKey_becomes_kafka_key()
    {
        var envelope = Make();
        envelope = envelope with { PartitionKey = "tenant-42" };

        var kafka = KafkaEnvelopeSerializer.ToKafka(envelope);

        Assert.Equal("tenant-42", kafka.Key);
    }

    [Fact]
    public void Body_becomes_kafka_value()
    {
        var envelope = Make();

        var kafka = KafkaEnvelopeSerializer.ToKafka(envelope);

        Assert.Equal(envelope.Body, kafka.Value);
    }

    [Fact]
    public void Missing_message_id_throws()
    {
        var envelope = Make();

        var kafka = KafkaEnvelopeSerializer.ToKafka(envelope);
        // Убираем обязательный заголовок — симулируем несовместимого продюсера.
        kafka.Headers.Remove("avtobus-message-id");

        Assert.Throws<InvalidDataException>(() => KafkaEnvelopeSerializer.FromKafka(ToConsumeResult(kafka)));
    }
}
