using System.Globalization;
using Azure.Messaging.ServiceBus;

namespace AvtoBus.AzureServiceBus;

/// <summary>
/// Маппинг <see cref="Envelope"/> на ServiceBusMessage: метаданные — в ApplicationProperties,
/// тело — в Body. Имена свойств совпадают со стандартом заголовков шины (идея 495).
/// </summary>
public static class AsbEnvelopeSerializer
{
    private const string MessageIdProperty = "avtobus-message-id";
    private const string CorrelationIdProperty = "avtobus-correlation-id";
    private const string CausationIdProperty = "avtobus-causation-id";
    private const string MessageTypeProperty = "avtobus-message-type";
    private const string ContentTypeProperty = "avtobus-content-type";
    private const string SentAtProperty = "avtobus-sent-at";
    private const string DeliverAtProperty = "avtobus-deliver-at";
    private const string TtlProperty = "avtobus-ttl";
    private const string PartitionKeyProperty = "avtobus-partition-key";
    private const string TenantIdProperty = "avtobus-tenant-id";
    private const string ReplyToProperty = "avtobus-reply-to";
    private const string TraceParentProperty = "avtobus-traceparent";

    private const string UserPropertyPrefix = "x-avb-";

    /// <summary>Записывает конверт в ServiceBusMessage для отправки.</summary>
    public static ServiceBusMessage ToMessage(Envelope envelope)
    {
        var message = new ServiceBusMessage(envelope.Body.ToArray())
        {
            MessageId = envelope.MessageId.ToString("N"),
            ContentType = envelope.ContentType,
            ScheduledEnqueueTime = envelope.DeliverAt?.UtcDateTime ?? default(DateTime),
        };

        message.ApplicationProperties[MessageIdProperty] = envelope.MessageId.ToString("N");
        message.ApplicationProperties[MessageTypeProperty] = envelope.MessageType;
        message.ApplicationProperties[ContentTypeProperty] = envelope.ContentType;
        message.ApplicationProperties[SentAtProperty] = envelope.SentAt.ToString("O", CultureInfo.InvariantCulture);

        if (envelope.CorrelationId is { } correlationId)
            message.ApplicationProperties[CorrelationIdProperty] = correlationId.ToString("N");
        if (envelope.CausationId is { } causationId)
            message.ApplicationProperties[CausationIdProperty] = causationId.ToString("N");
        if (envelope.DeliverAt is { } deliverAt)
            message.ApplicationProperties[DeliverAtProperty] = deliverAt.ToString("O", CultureInfo.InvariantCulture);
        if (envelope.TimeToLive is { } ttl)
            message.ApplicationProperties[TtlProperty] = ttl.ToString("c", CultureInfo.InvariantCulture);
        if (envelope.PartitionKey is { } partitionKey)
            message.ApplicationProperties[PartitionKeyProperty] = partitionKey;
        if (envelope.TenantId is { } tenantId)
            message.ApplicationProperties[TenantIdProperty] = tenantId;
        if (envelope.ReplyTo is { } replyTo)
            message.ApplicationProperties[ReplyToProperty] = replyTo;
        if (envelope.TraceParent is { } traceParent)
            message.ApplicationProperties[TraceParentProperty] = traceParent;

        foreach (var (key, value) in envelope.Headers)
            message.ApplicationProperties[UserPropertyPrefix + key] = value;

        return message;
    }

    /// <summary>Восстанавливает конверт из полученного ServiceBusReceivedMessage.</summary>
    public static Envelope FromMessage(ServiceBusReceivedMessage message)
    {
        var properties = message.ApplicationProperties;
        string Get(string key) => properties.TryGetValue(key, out var value) ? value?.ToString() ?? string.Empty : string.Empty;

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in properties)
        {
            if (key.StartsWith(UserPropertyPrefix, StringComparison.Ordinal))
                userHeaders[key[UserPropertyPrefix.Length..]] = value?.ToString() ?? string.Empty;
        }

        return new Envelope
        {
            MessageId = Guid.TryParse(Get(MessageIdProperty), out var messageId)
                ? messageId
                : throw new InvalidDataException("ASB-сообщение без свойства 'avtobus-message-id'."),
            CorrelationId = ParseNullableGuid(Get(CorrelationIdProperty)),
            CausationId = ParseNullableGuid(Get(CausationIdProperty)),
            MessageType = Get(MessageTypeProperty)
                          ?? throw new InvalidDataException("ASB-сообщение без свойства 'avtobus-message-type'."),
            Body = message.Body.ToArray(),
            ContentType = Get(ContentTypeProperty) is { Length: > 0 } ct ? ct : "application/json",
            SentAt = ParseNullableDateTimeOffset(Get(SentAtProperty)) ?? DateTimeOffset.UtcNow,
            DeliverAt = ParseNullableDateTimeOffset(Get(DeliverAtProperty)),
            TimeToLive = ParseNullableTimeSpan(Get(TtlProperty)),
            PartitionKey = Get(PartitionKeyProperty),
            TenantId = Get(TenantIdProperty),
            ReplyTo = Get(ReplyToProperty),
            DeliveryAttempt = message.DeliveryCount > 0 ? message.DeliveryCount : 1,
            TraceParent = Get(TraceParentProperty),
            Headers = userHeaders.ToDictionary(StringComparer.Ordinal),
        };
    }

    private static Guid? ParseNullableGuid(string? value)
        => value is not null && Guid.TryParse(value, out var parsed) ? parsed : null;

    private static DateTimeOffset? ParseNullableDateTimeOffset(string? value)
        => value is not null
           && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;

    private static TimeSpan? ParseNullableTimeSpan(string? value)
        => value is not null && TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
}
