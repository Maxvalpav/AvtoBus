using System.Collections.Frozen;
using System.Globalization;
using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace AvtoBus.Nats;

/// <summary>
/// Маппинг <see cref="Envelope"/> на NATS-сообщение: метаданные — в заголовки, тело — в Data.
/// Имена заголовков совпадают со стандартом шины (идея 495), поэтому конверт читаем
/// любым NATS-тулом.
/// </summary>
public static class NatsEnvelopeSerializer
{
    private const string MessageIdHeader = "avtobus-message-id";
    private const string CorrelationIdHeader = "avtobus-correlation-id";
    private const string CausationIdHeader = "avtobus-causation-id";
    private const string MessageTypeHeader = "avtobus-message-type";
    private const string ContentTypeHeader = "avtobus-content-type";
    private const string SentAtHeader = "avtobus-sent-at";
    private const string DeliverAtHeader = "avtobus-deliver-at";
    private const string TtlHeader = "avtobus-ttl";
    private const string PartitionKeyHeader = "avtobus-partition-key";
    private const string TenantIdHeader = "avtobus-tenant-id";
    private const string ReplyToHeader = "avtobus-reply-to";
    private const string AttemptHeader = "avtobus-attempt";
    private const string TraceParentHeader = "avtobus-traceparent";

    private const string UserHeaderPrefix = "x-avb-";

    /// <summary>Записывает конверт в заголовки + тело для публикации в JetStream.</summary>
    public static (NatsHeaders Headers, byte[] Body) ToNats(Envelope envelope)
    {
        var headers = new NatsHeaders
        {
            [MessageIdHeader] = envelope.MessageId.ToString("N"),
            [MessageTypeHeader] = envelope.MessageType,
            [ContentTypeHeader] = envelope.ContentType,
            [SentAtHeader] = envelope.SentAt.ToString("O", CultureInfo.InvariantCulture),
            [AttemptHeader] = envelope.DeliveryAttempt.ToString(CultureInfo.InvariantCulture),
        };

        if (envelope.CorrelationId is { } correlationId)
            headers[CorrelationIdHeader] = correlationId.ToString("N");
        if (envelope.CausationId is { } causationId)
            headers[CausationIdHeader] = causationId.ToString("N");
        if (envelope.DeliverAt is { } deliverAt)
            headers[DeliverAtHeader] = deliverAt.ToString("O", CultureInfo.InvariantCulture);
        if (envelope.TimeToLive is { } ttl)
            headers[TtlHeader] = ttl.ToString("c", CultureInfo.InvariantCulture);
        if (envelope.PartitionKey is { } partitionKey)
            headers[PartitionKeyHeader] = partitionKey;
        if (envelope.TenantId is { } tenantId)
            headers[TenantIdHeader] = tenantId;
        if (envelope.ReplyTo is { } replyTo)
            headers[ReplyToHeader] = replyTo;
        if (envelope.TraceParent is { } traceParent)
            headers[TraceParentHeader] = traceParent;

        foreach (var (key, value) in envelope.Headers)
            headers[UserHeaderPrefix + key] = value;

        return (headers, envelope.Body.ToArray());
    }

    /// <summary>
    /// Восстанавливает конверт из JetStream-сообщения. DeliveryAttempt берётся из JetStream
    /// metadata (NumDelivered) — нативный счётчик ретраев; при его отсутствии — из заголовка.
    /// </summary>
    public static Envelope FromNats(NatsJSMsg<byte[]> message)
    {
        var headers = message.Headers ?? new NatsHeaders();
        string Get(string key) => headers.TryGetLastValue(key, out var v) ? v! : string.Empty;

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var key in headers.Keys)
        {
            if (key.StartsWith(UserHeaderPrefix, StringComparison.Ordinal) && headers.TryGetLastValue(key, out var v))
                userHeaders[key[UserHeaderPrefix.Length..]] = v!;
        }

        var delivered = (int)(message.Metadata?.NumDelivered ?? 0);
        var attempt = delivered > 0
            ? delivered
            : int.TryParse(Get(AttemptHeader), NumberStyles.Integer, CultureInfo.InvariantCulture, out var h)
                ? h
                : 1;

        return new Envelope
        {
            MessageId = Guid.TryParse(Get(MessageIdHeader), out var messageId)
                ? messageId
                : throw new InvalidDataException($"NATS-сообщение без заголовка '{MessageIdHeader}'."),
            CorrelationId = ParseNullableGuid(Get(CorrelationIdHeader)),
            CausationId = ParseNullableGuid(Get(CausationIdHeader)),
            MessageType = Get(MessageTypeHeader)
                          ?? throw new InvalidDataException($"NATS-сообщение без заголовка '{MessageTypeHeader}'."),
            Body = message.Data,
            ContentType = Get(ContentTypeHeader) is { Length: > 0 } ct ? ct : "application/json",
            SentAt = ParseNullableDateTimeOffset(Get(SentAtHeader))
                     ?? DateTimeOffset.FromUnixTimeMilliseconds((long)(message.Metadata?.Timestamp.ToUnixTimeMilliseconds() ?? 0)),
            DeliverAt = ParseNullableDateTimeOffset(Get(DeliverAtHeader)),
            TimeToLive = ParseNullableTimeSpan(Get(TtlHeader)),
            PartitionKey = Get(PartitionKeyHeader),
            TenantId = Get(TenantIdHeader),
            ReplyTo = Get(ReplyToHeader),
            DeliveryAttempt = attempt,
            TraceParent = Get(TraceParentHeader),
            Headers = userHeaders.ToFrozenDictionary(StringComparer.Ordinal),
        };
    }

    private static Guid? ParseNullableGuid(string? value)
        => Guid.TryParse(value, out var parsed) ? parsed : null;

    private static DateTimeOffset? ParseNullableDateTimeOffset(string? value)
        => DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;

    private static TimeSpan? ParseNullableTimeSpan(string? value)
        => TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
}
