using AvtoBus.AzureServiceBus;
using Azure.Messaging.ServiceBus;

namespace AvtoBus.Tests;

/// <summary>
/// Юнит-тесты маппинга Envelope ↔ ServiceBusMessage без Azure.
/// </summary>
public sealed class AsbEnvelopeSerializerTests
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

    private static ServiceBusReceivedMessage ToReceived(ServiceBusMessage message)
        => ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: message.Body,
            messageId: message.MessageId,
            contentType: message.ContentType,
            scheduledEnqueueTime: message.ScheduledEnqueueTime,
            properties: message.ApplicationProperties);

    [Fact]
    public void RoundTrip_preserves_envelope_metadata_and_body()
    {
        var envelope = Make();

        var restored = AsbEnvelopeSerializer.FromMessage(ToReceived(AsbEnvelopeSerializer.ToMessage(envelope)));

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
    public void RoundTrip_preserves_user_headers()
    {
        var envelope = Make().WithHeader("x-test", "42").WithHeader("x-other", "∅");

        var restored = AsbEnvelopeSerializer.FromMessage(ToReceived(AsbEnvelopeSerializer.ToMessage(envelope)));

        Assert.Equal("42", restored.Header("x-test"));
        Assert.Equal("∅", restored.Header("x-other"));
    }

    [Fact]
    public void Body_is_stored_and_restored_byte_exact()
    {
        var envelope = Make();
        envelope = envelope with { Body = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF } };

        var restored = AsbEnvelopeSerializer.FromMessage(ToReceived(AsbEnvelopeSerializer.ToMessage(envelope)));

        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, restored.Body);
    }

    [Fact]
    public void Missing_message_id_throws()
    {
        var message = AsbEnvelopeSerializer.ToMessage(Make());
        message.ApplicationProperties.Remove("avtobus-message-id");

        Assert.Throws<InvalidDataException>(
            () => AsbEnvelopeSerializer.FromMessage(ToReceived(message)));
    }

    [Fact]
    public void Delivery_attempt_comes_from_DeliveryCount()
    {
        var message = AsbEnvelopeSerializer.ToMessage(Make());
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: message.Body,
            messageId: message.MessageId,
            contentType: message.ContentType,
            properties: message.ApplicationProperties,
            deliveryCount: 5);

        var restored = AsbEnvelopeSerializer.FromMessage(received);

        Assert.Equal(5, restored.DeliveryAttempt);
    }
}
