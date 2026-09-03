using System.Collections.Frozen;
using System.Globalization;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AvtoBus.RabbitMq;

/// <summary>
/// Маппинг <see cref="Envelope"/> на AMQP-сообщение: служебные поля — в BasicProperties
/// (message-id, correlation-id, type, reply-to, timestamp, ttl), остальные метаданные — в
/// заголовки, тело — в body. Имена заголовков совпадают со стандартом шины (идея 495),
/// поэтому конверт читаем любым AMQP-тулом.
/// </summary>
public static class RabbitMqEnvelopeSerializer
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

    /// <summary>
    /// Нативный счётчик доставок quorum queue (добавляется брокером на повторной доставке).
    /// Для попыток предпочитаем его заголовку <see cref="AttemptHeader"/>.
    /// </summary>
    private const string DeliveryCountHeader = "x-delivery-count";

    /// <summary>Префикс для пользовательских заголовков конверта, чтобы не смешивать со служебными.</summary>
    private const string UserHeaderPrefix = "x-avb-";

    /// <summary>
    /// Записывает конверт в AMQP <see cref="BasicProperties"/> + тело. Публикация идёт
    /// с publisher confirms, поэтому persistent-сообщения не теряются при рестарте брокера.
    /// </summary>
    public static (BasicProperties Properties, ReadOnlyMemory<byte> Body) ToRabbitMq(Envelope envelope)
    {
        var headers = new Dictionary<string, object?>
        {
            [MessageIdHeader] = Bytes(envelope.MessageId.ToString("N")),
            [MessageTypeHeader] = Bytes(envelope.MessageType),
            [ContentTypeHeader] = Bytes(envelope.ContentType),
            [SentAtHeader] = Bytes(envelope.SentAt.ToString("O", CultureInfo.InvariantCulture)),
            [AttemptHeader] = Bytes(envelope.DeliveryAttempt.ToString(CultureInfo.InvariantCulture)),
        };

        if (envelope.CorrelationId is { } correlationId)
            headers[CorrelationIdHeader] = Bytes(correlationId.ToString("N"));
        if (envelope.CausationId is { } causationId)
            headers[CausationIdHeader] = Bytes(causationId.ToString("N"));
        if (envelope.DeliverAt is { } deliverAt)
            headers[DeliverAtHeader] = Bytes(deliverAt.ToString("O", CultureInfo.InvariantCulture));
        if (envelope.TimeToLive is { } ttl)
            headers[TtlHeader] = Bytes(ttl.ToString("c", CultureInfo.InvariantCulture));
        if (envelope.PartitionKey is { } partitionKey)
            headers[PartitionKeyHeader] = Bytes(partitionKey);
        if (envelope.TenantId is { } tenantId)
            headers[TenantIdHeader] = Bytes(tenantId);
        if (envelope.ReplyTo is { } replyTo)
            headers[ReplyToHeader] = Bytes(replyTo);
        if (envelope.TraceParent is { } traceParent)
            headers[TraceParentHeader] = Bytes(traceParent);

        foreach (var (key, value) in envelope.Headers)
            headers[UserHeaderPrefix + key] = Bytes(value);

        var properties = new BasicProperties
        {
            Persistent = true,
            MessageId = envelope.MessageId.ToString("N"),
            CorrelationId = envelope.CorrelationId?.ToString("N"),
            ReplyTo = envelope.ReplyTo,
            Type = envelope.MessageType,
            ContentType = envelope.ContentType,
            Timestamp = new AmqpTimestamp(envelope.SentAt.ToUnixTimeSeconds()),
            Headers = headers,
        };

        if (envelope.TimeToLive is { } timeToLive)
            properties.Expiration = ((long)timeToLive.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);

        return (properties, envelope.Body);
    }

    /// <summary>
    /// Восстанавливает конверт из AMQP-доставки. Недостающие обязательные поля (message-id,
    /// message-type) означают несовместимого продюсера — бросаем <see cref="InvalidDataException"/>.
    /// DeliveryAttempt берётся из нативного <c>x-delivery-count</c> (при наличии), иначе — из
    /// заголовка <see cref="AttemptHeader"/>.
    /// </summary>
    public static Envelope FromRabbitMq(BasicDeliverEventArgs args)
    {
        var properties = args.BasicProperties;
        var values = ReadHeaderValues(properties);

        var deliveryCount = ParseNullableInt(values.GetValueOrDefault(DeliveryCountHeader));
        var attempt = deliveryCount is > 0
            ? deliveryCount.Value
            : ParseNullableInt(values.GetValueOrDefault(AttemptHeader)) is { } headerAttempt
                ? headerAttempt
                : 1;

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in values)
        {
            if (key.StartsWith(UserHeaderPrefix, StringComparison.Ordinal))
                userHeaders[key[UserHeaderPrefix.Length..]] = value;
        }

        var messageId = ParseNullableGuid(properties.MessageId)
                        ?? ParseNullableGuid(values.GetValueOrDefault(MessageIdHeader));
        var messageType = properties.Type ?? values.GetValueOrDefault(MessageTypeHeader);

        if (messageId is null)
            throw new InvalidDataException($"AMQP-сообщение без идентификатора (message-id/header '{MessageIdHeader}').");
        if (messageType is null)
            throw new InvalidDataException($"AMQP-сообщение без типа (properties.Type/header '{MessageTypeHeader}').");

        var sentAt = ParseNullableDateTimeOffset(values.GetValueOrDefault(SentAtHeader))
                     ?? FromAmqpTimestamp(properties.Timestamp)
                     ?? DateTimeOffset.UtcNow;

        return new Envelope
        {
            MessageId = messageId.Value,
            CorrelationId = ParseNullableGuid(properties.CorrelationId)
                            ?? ParseNullableGuid(values.GetValueOrDefault(CorrelationIdHeader)),
            CausationId = ParseNullableGuid(values.GetValueOrDefault(CausationIdHeader)),
            MessageType = messageType,
            Body = args.Body,
            ContentType = properties.ContentType ?? values.GetValueOrDefault(ContentTypeHeader) ?? "application/json",
            SentAt = sentAt,
            DeliverAt = ParseNullableDateTimeOffset(values.GetValueOrDefault(DeliverAtHeader)),
            TimeToLive = ParseNullableTimeSpan(values.GetValueOrDefault(TtlHeader)),
            PartitionKey = values.GetValueOrDefault(PartitionKeyHeader),
            TenantId = values.GetValueOrDefault(TenantIdHeader),
            ReplyTo = properties.ReplyTo ?? values.GetValueOrDefault(ReplyToHeader),
            DeliveryAttempt = attempt,
            TraceParent = values.GetValueOrDefault(TraceParentHeader),
            Headers = userHeaders.ToFrozenDictionary(StringComparer.Ordinal),
        };
    }

    /// <summary>
    /// Заголовки AMQP могут быть byte[], string или скалярами — приводим всё к строке,
    /// чтобы конверт не зависел от типа, выбранного продюсером.
    /// </summary>
    private static Dictionary<string, string> ReadHeaderValues(IReadOnlyBasicProperties properties)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        if (properties.Headers is not { } headers)
            return values;

        foreach (var (key, value) in headers)
        {
            values[key] = value switch
            {
                byte[] bytes => Encoding.UTF8.GetString(bytes),
                string text => text,
                null => string.Empty,
                _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
            };
        }

        return values;
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static DateTimeOffset? FromAmqpTimestamp(AmqpTimestamp timestamp)
        => timestamp.UnixTime > 0 ? DateTimeOffset.FromUnixTimeSeconds(timestamp.UnixTime) : null;

    private static Guid? ParseNullableGuid(string? value)
        => Guid.TryParse(value, out var parsed) ? parsed : null;

    private static DateTimeOffset? ParseNullableDateTimeOffset(string? value)
        => DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;

    private static TimeSpan? ParseNullableTimeSpan(string? value)
        => TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;

    private static int? ParseNullableInt(string? value)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
}
