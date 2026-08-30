using System.Collections.Frozen;
using System.Globalization;
using System.Text;

namespace AvtoBus.Kafka;

/// <summary>
/// Маппинг <see cref="Envelope"/> на Kafka: метаданные — в заголовки сообщения, тело — в value.
/// Заголовки читаемы внешними инструментами (идея 495: стандарт заголовков).
/// </summary>
public static class KafkaEnvelopeSerializer
{
    // Служебные заголовки (внутренние метаданные конверта).
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

    /// <summary>Префикс для пользовательских заголовков конверта, чтобы не смешивать со служебными.</summary>
    private const string UserHeaderPrefix = "x-avb-";

    /// <summary>
    /// Записывает конверт в <see cref="Confluent.Kafka.Message{TKey,TValue}"/>.
    /// value = тело сообщения, headers = метаданные + пользовательские заголовки конверта.
    /// </summary>
    public static Confluent.Kafka.Message<string, byte[]> ToKafka(Envelope envelope)
    {
        var headers = new Confluent.Kafka.Headers
        {
            new(MessageIdHeader, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("N"))),
            new(MessageTypeHeader, Encoding.UTF8.GetBytes(envelope.MessageType)),
            new(ContentTypeHeader, Encoding.UTF8.GetBytes(envelope.ContentType)),
            new(SentAtHeader, Encoding.UTF8.GetBytes(envelope.SentAt.ToString("O", CultureInfo.InvariantCulture))),
            new(AttemptHeader, Encoding.UTF8.GetBytes(envelope.DeliveryAttempt.ToString(CultureInfo.InvariantCulture))),
        };

        if (envelope.CorrelationId is { } correlationId)
            headers.Add(new(CorrelationIdHeader, Encoding.UTF8.GetBytes(correlationId.ToString("N"))));

        if (envelope.CausationId is { } causationId)
            headers.Add(new(CausationIdHeader, Encoding.UTF8.GetBytes(causationId.ToString("N"))));

        if (envelope.DeliverAt is { } deliverAt)
            headers.Add(new(DeliverAtHeader, Encoding.UTF8.GetBytes(deliverAt.ToString("O", CultureInfo.InvariantCulture))));

        if (envelope.TimeToLive is { } ttl)
            headers.Add(new(TtlHeader, Encoding.UTF8.GetBytes(ttl.ToString("c", CultureInfo.InvariantCulture))));

        if (envelope.PartitionKey is { } partitionKey)
            headers.Add(new(PartitionKeyHeader, Encoding.UTF8.GetBytes(partitionKey)));

        if (envelope.TenantId is { } tenantId)
            headers.Add(new(TenantIdHeader, Encoding.UTF8.GetBytes(tenantId)));

        if (envelope.ReplyTo is { } replyTo)
            headers.Add(new(ReplyToHeader, Encoding.UTF8.GetBytes(replyTo)));

        if (envelope.TraceParent is { } traceParent)
            headers.Add(new(TraceParentHeader, Encoding.UTF8.GetBytes(traceParent)));

        foreach (var (key, value) in envelope.Headers)
            headers.Add(new(UserHeaderPrefix + key, Encoding.UTF8.GetBytes(value)));

        return new Confluent.Kafka.Message<string, byte[]>
        {
            Key = envelope.PartitionKey ?? envelope.MessageId.ToString("N"),
            Value = envelope.Body.ToArray(),
            Headers = headers,
            Timestamp = new Confluent.Kafka.Timestamp(envelope.SentAt),
        };
    }

    /// <summary>
    /// Восстанавливает конверт из Kafka. Недостающие обязательные заголовки (message-id,
    /// message-type, content-type, sent-at) фолбэком берут из value-обёртки нечего —
    /// их отсутствие означает несовместимого продюсера, поэтому бросаем <see cref="InvalidDataException"/>.
    /// </summary>
    public static Envelope FromKafka(Confluent.Kafka.ConsumeResult<string, byte[]> result)
    {
        var headers = result.Message.Headers;
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in headers)
        {
            if (header.GetValueBytes() is { } bytes)
                values[header.Key] = Encoding.UTF8.GetString(bytes);
        }

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in values)
        {
            if (key.StartsWith(UserHeaderPrefix, StringComparison.Ordinal))
                userHeaders[key[UserHeaderPrefix.Length..]] = value;
        }

        return new Envelope
        {
            MessageId = Guid.TryParse(values.GetValueOrDefault(MessageIdHeader), out var messageId)
                ? messageId
                : throw new InvalidDataException($"Kafka-сообщение без заголовка '{MessageIdHeader}'."),
            CorrelationId = ParseNullableGuid(values.GetValueOrDefault(CorrelationIdHeader)),
            CausationId = ParseNullableGuid(values.GetValueOrDefault(CausationIdHeader)),
            MessageType = values.GetValueOrDefault(MessageTypeHeader)
                          ?? throw new InvalidDataException($"Kafka-сообщение без заголовка '{MessageTypeHeader}'."),
            Body = result.Message.Value,
            ContentType = values.GetValueOrDefault(ContentTypeHeader) ?? "application/json",
            SentAt = ParseNullableDateTimeOffset(values.GetValueOrDefault(SentAtHeader))
                     ?? DateTimeOffset.FromUnixTimeMilliseconds(result.Message.Timestamp.UnixTimestampMs),
            DeliverAt = ParseNullableDateTimeOffset(values.GetValueOrDefault(DeliverAtHeader)),
            TimeToLive = ParseNullableTimeSpan(values.GetValueOrDefault(TtlHeader)),
            PartitionKey = values.GetValueOrDefault(PartitionKeyHeader),
            TenantId = values.GetValueOrDefault(TenantIdHeader),
            ReplyTo = values.GetValueOrDefault(ReplyToHeader),
            DeliveryAttempt = int.TryParse(
                values.GetValueOrDefault(AttemptHeader),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var attempt)
                ? attempt
                : 1,
            TraceParent = values.GetValueOrDefault(TraceParentHeader),
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
