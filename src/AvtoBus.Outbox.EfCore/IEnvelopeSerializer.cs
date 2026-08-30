using System.Buffers;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Сериализация конверта в blob для хранения в БД и обратно (док 15, §2).
/// </summary>
public interface IEnvelopeSerializer
{
    byte[] Serialize(Envelope env);

    Envelope Deserialize(ReadOnlyMemory<byte> blob);
}

/// <summary>System.Text.Json — не требует MemoryPack, тело конверта копируется как byte[].</summary>
public sealed class JsonEnvelopeSerializer : IEnvelopeSerializer
{
    private static readonly System.Text.Json.JsonSerializerOptions Options = new(System.Text.Json.JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    public byte[] Serialize(Envelope env)
    {
        var dto = new EnvelopeData
        {
            MessageId = env.MessageId,
            CorrelationId = env.CorrelationId,
            CausationId = env.CausationId,
            MessageType = env.MessageType,
            Body = env.Body.ToArray(),
            ContentType = env.ContentType,
            SentAt = env.SentAt,
            DeliverAt = env.DeliverAt,
            TimeToLive = env.TimeToLive,
            PartitionKey = env.PartitionKey,
            TenantId = env.TenantId,
            ReplyTo = env.ReplyTo,
            DeliveryAttempt = env.DeliveryAttempt,
            TraceParent = env.TraceParent,
            Headers = env.Headers,
        };

        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(dto, Options);
    }

    public Envelope Deserialize(ReadOnlyMemory<byte> blob)
    {
        var dto = System.Text.Json.JsonSerializer.Deserialize<EnvelopeData>(blob.Span, Options)
            ?? throw new InvalidOperationException("Конверт не десериализовался из outbox-бд.");

        return new Envelope
        {
            MessageId = dto.MessageId,
            CorrelationId = dto.CorrelationId,
            CausationId = dto.CausationId,
            MessageType = dto.MessageType,
            Body = dto.Body,
            ContentType = dto.ContentType,
            SentAt = dto.SentAt,
            DeliverAt = dto.DeliverAt,
            TimeToLive = dto.TimeToLive,
            PartitionKey = dto.PartitionKey,
            TenantId = dto.TenantId,
            ReplyTo = dto.ReplyTo,
            DeliveryAttempt = dto.DeliveryAttempt,
            TraceParent = dto.TraceParent,
            Headers = dto.Headers ?? System.Collections.Frozen.FrozenDictionary<string, string>.Empty,
        };
    }

    private sealed class EnvelopeData
    {
        public Guid MessageId { get; set; }
        public Guid? CorrelationId { get; set; }
        public Guid? CausationId { get; set; }
        public string MessageType { get; set; } = "";
        public byte[] Body { get; set; } = [];
        public string ContentType { get; set; } = "application/json";
        public DateTimeOffset SentAt { get; set; }
        public DateTimeOffset? DeliverAt { get; set; }
        public TimeSpan? TimeToLive { get; set; }
        public string? PartitionKey { get; set; }
        public string? TenantId { get; set; }
        public string? ReplyTo { get; set; }
        public int DeliveryAttempt { get; set; } = 1;
        public string? TraceParent { get; set; }
        public IReadOnlyDictionary<string, string>? Headers { get; set; }
    }
}
