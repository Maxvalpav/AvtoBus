using System.Globalization;
using System.Text;
using StackExchange.Redis;

namespace AvtoBus.Redis;

/// <summary>
/// Маппинг <see cref="Envelope"/> на поля Redis Stream записи: метаданные — отдельные поля,
/// тело — поле <c>body</c> (base64). Имена полей совпадают со стандартом заголовков шины (идея 495).
/// </summary>
public static class RedisEnvelopeSerializer
{
    private const string MessageIdField = "avtobus-message-id";
    private const string CorrelationIdField = "avtobus-correlation-id";
    private const string CausationIdField = "avtobus-causation-id";
    private const string MessageTypeField = "avtobus-message-type";
    private const string ContentTypeField = "avtobus-content-type";
    private const string SentAtField = "avtobus-sent-at";
    private const string DeliverAtField = "avtobus-deliver-at";
    private const string TtlField = "avtobus-ttl";
    private const string PartitionKeyField = "avtobus-partition-key";
    private const string TenantIdField = "avtobus-tenant-id";
    private const string ReplyToField = "avtobus-reply-to";
    private const string AttemptField = "avtobus-attempt";
    private const string TraceParentField = "avtobus-traceparent";
    private const string BodyField = "body";

    private const string UserFieldPrefix = "x-avb-";

    /// <summary>Записывает конверт в поля Redis Stream записи.</summary>
    public static NameValueEntry[] ToEntries(Envelope envelope)
    {
        var entries = new List<NameValueEntry>
        {
            new(MessageIdField, envelope.MessageId.ToString("N")),
            new(MessageTypeField, envelope.MessageType),
            new(ContentTypeField, envelope.ContentType),
            new(SentAtField, envelope.SentAt.ToString("O", CultureInfo.InvariantCulture)),
            new(AttemptField, envelope.DeliveryAttempt.ToString(CultureInfo.InvariantCulture)),
            new(BodyField, Convert.ToBase64String(envelope.Body.Span)),
        };

        if (envelope.CorrelationId is { } correlationId)
            entries.Add(new(CorrelationIdField, correlationId.ToString("N")));
        if (envelope.CausationId is { } causationId)
            entries.Add(new(CausationIdField, causationId.ToString("N")));
        if (envelope.DeliverAt is { } deliverAt)
            entries.Add(new(DeliverAtField, deliverAt.ToString("O", CultureInfo.InvariantCulture)));
        if (envelope.TimeToLive is { } ttl)
            entries.Add(new(TtlField, ttl.ToString("c", CultureInfo.InvariantCulture)));
        if (envelope.PartitionKey is { } partitionKey)
            entries.Add(new(PartitionKeyField, partitionKey));
        if (envelope.TenantId is { } tenantId)
            entries.Add(new(TenantIdField, tenantId));
        if (envelope.ReplyTo is { } replyTo)
            entries.Add(new(ReplyToField, replyTo));
        if (envelope.TraceParent is { } traceParent)
            entries.Add(new(TraceParentField, traceParent));

        foreach (var (key, value) in envelope.Headers)
            entries.Add(new(UserFieldPrefix + key, value));

        return entries.ToArray();
    }

    /// <summary>Восстанавливает конверт из полей записи стрима.</summary>
    public static Envelope FromEntry(StreamEntry entry)
    {
        var values = new Dictionary<string, RedisValue>(StringComparer.Ordinal);
        foreach (var field in entry.Values)
        {
            if (field.Name.ToString() is { Length: > 0 } name)
                values[name] = field.Value;
        }

        string Get(string key) => values.TryGetValue(key, out var v) ? v.ToString() ?? string.Empty : string.Empty;

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in values)
        {
            if (key.StartsWith(UserFieldPrefix, StringComparison.Ordinal))
                userHeaders[key[UserFieldPrefix.Length..]] = value.ToString() ?? string.Empty;
        }

        var bodyBase64 = Get(BodyField);

        return new Envelope
        {
            MessageId = Guid.TryParse(Get(MessageIdField), out var messageId)
                ? messageId
                : throw new InvalidDataException($"Redis-сообщение без поля '{MessageIdField}'."),
            CorrelationId = ParseNullableGuid(Get(CorrelationIdField)),
            CausationId = ParseNullableGuid(Get(CausationIdField)),
            MessageType = Get(MessageTypeField)
                          ?? throw new InvalidDataException($"Redis-сообщение без поля '{MessageTypeField}'."),
            Body = string.IsNullOrEmpty(bodyBase64) ? Array.Empty<byte>() : Convert.FromBase64String(bodyBase64),
            ContentType = Get(ContentTypeField) is { Length: > 0 } ct ? ct : "application/json",
            SentAt = ParseNullableDateTimeOffset(Get(SentAtField)) ?? DateTimeOffset.UtcNow,
            DeliverAt = ParseNullableDateTimeOffset(Get(DeliverAtField)),
            TimeToLive = ParseNullableTimeSpan(Get(TtlField)),
            PartitionKey = Get(PartitionKeyField),
            TenantId = Get(TenantIdField),
            ReplyTo = Get(ReplyToField),
            DeliveryAttempt = int.TryParse(Get(AttemptField), NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempt)
                ? attempt
                : 1,
            TraceParent = Get(TraceParentField),
            Headers = userHeaders.ToDictionary(StringComparer.Ordinal),
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
