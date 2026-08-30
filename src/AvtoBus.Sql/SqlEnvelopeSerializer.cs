using System.Globalization;
using System.Text;
using System.Text.Json;

namespace AvtoBus.Sql;

/// <summary>
/// Маппинг <see cref="Envelope"/> на BYTEA-блоб таблицы-очереди: компактный JSON
/// (метаданные полями, тело — base64). Имена полей совпадают со стандартом заголовков шины (идея 495).
/// </summary>
public static class SqlEnvelopeSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>Сериализует конверт в BYTEA для хранения в таблице.</summary>
    public static byte[] ToBlob(Envelope envelope)
    {
        var dto = new EnvelopeDto
        {
            MessageId = envelope.MessageId.ToString("N"),
            CorrelationId = envelope.CorrelationId?.ToString("N"),
            CausationId = envelope.CausationId?.ToString("N"),
            MessageType = envelope.MessageType,
            ContentType = envelope.ContentType,
            SentAt = envelope.SentAt.ToString("O", CultureInfo.InvariantCulture),
            DeliverAt = envelope.DeliverAt?.ToString("O", CultureInfo.InvariantCulture),
            Ttl = envelope.TimeToLive?.ToString("c", CultureInfo.InvariantCulture),
            PartitionKey = envelope.PartitionKey,
            TenantId = envelope.TenantId,
            ReplyTo = envelope.ReplyTo,
            TraceParent = envelope.TraceParent,
            Attempt = envelope.DeliveryAttempt,
            Body = Convert.ToBase64String(envelope.Body.Span),
            Headers = envelope.Headers.ToDictionary(StringComparer.Ordinal),
        };

        return JsonSerializer.SerializeToUtf8Bytes(dto, Options);
    }

    /// <summary>Восстанавливает конверт из BYTEA-блоба.</summary>
    public static Envelope FromBlob(byte[] blob)
    {
        EnvelopeDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<EnvelopeDto>(blob, Options);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("SQL-сообщение не является корректным JSON.", exception);
        }

        if (dto is null)
            throw new InvalidDataException("SQL-сообщение пустое.");

        if (dto.MessageId is null)
            throw new InvalidDataException("SQL-сообщение без 'messageId'.");
        if (dto.MessageType is null)
            throw new InvalidDataException("SQL-сообщение без 'messageType'.");

        return new Envelope
        {
            MessageId = Guid.TryParse(dto.MessageId, out var messageId)
                ? messageId
                : throw new InvalidDataException("SQL-сообщение с некорректным 'messageId'."),
            CorrelationId = ParseNullableGuid(dto.CorrelationId),
            CausationId = ParseNullableGuid(dto.CausationId),
            MessageType = dto.MessageType,
            Body = string.IsNullOrEmpty(dto.Body) ? Array.Empty<byte>() : Convert.FromBase64String(dto.Body),
            ContentType = dto.ContentType is { Length: > 0 } ct ? ct : "application/json",
            SentAt = ParseNullableDateTimeOffset(dto.SentAt) ?? DateTimeOffset.UtcNow,
            DeliverAt = ParseNullableDateTimeOffset(dto.DeliverAt),
            TimeToLive = ParseNullableTimeSpan(dto.Ttl),
            PartitionKey = dto.PartitionKey,
            TenantId = dto.TenantId,
            ReplyTo = dto.ReplyTo,
            TraceParent = dto.TraceParent,
            DeliveryAttempt = dto.Attempt is { } attempt && attempt > 0 ? attempt : 1,
            Headers = dto.Headers?.ToDictionary(StringComparer.Ordinal)
                      ?? new Dictionary<string, string>(StringComparer.Ordinal),
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

    private sealed class EnvelopeDto
    {
        public string? MessageId { get; set; }
        public string? CorrelationId { get; set; }
        public string? CausationId { get; set; }
        public string? MessageType { get; set; }
        public string? ContentType { get; set; }
        public string? SentAt { get; set; }
        public string? DeliverAt { get; set; }
        public string? Ttl { get; set; }
        public string? PartitionKey { get; set; }
        public string? TenantId { get; set; }
        public string? ReplyTo { get; set; }
        public string? TraceParent { get; set; }
        public int? Attempt { get; set; }
        public string? Body { get; set; }
        public Dictionary<string, string>? Headers { get; set; }
    }
}
