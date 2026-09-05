using System.Buffers;
using AvtoBus.Outbox.EfCore;
using AvtoBus.Security;
using FsCheck;
using FsCheck.Fluent;
using Google.Protobuf.WellKnownTypes;

namespace AvtoBus.Tests;

/// <summary>
/// Property-based тесты сериализации и подписи (аудит 03 §2.2, FsCheck):
/// round-trip «конверт → байты → конверт» и инварианты подписи.
/// </summary>
public class SerializationPropertyTests
{
    private static readonly SecurityKeys PropertyKeys =
        SecurityKeys.FromSecret("property-test-secret-only", iterations: 1_000);

    private static EnvelopeSecurity Verifier() => new(new SecurityOptions
    {
        RequireSignature = true,
        SignatureVersion = 3,
        MaxSignatureAge = TimeSpan.FromHours(1),
    }.WithKeys(PropertyKeys));

    [Fact]
    public void Json_envelope_round_trip()
    {
        Check.QuickThrowOnFailure(Prop.ForAll<(string, byte[], int, string)>(t =>
        {
            var (messageType, body, priority, partitionKey) = t;
            var serializer = new JsonEnvelopeSerializer();
            var original = new Envelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = Guid.NewGuid(),
                MessageType = messageType,
                Body = body,
                SentAt = DateTimeOffset.UtcNow,
                DeliverAt = DateTimeOffset.UtcNow.AddMinutes(5),
                TimeToLive = TimeSpan.FromMinutes(30),
                PartitionKey = partitionKey,
                TenantId = "acme",
                Priority = priority,
                ReplyTo = "replies",
                DeliveryAttempt = 1,
                TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                Headers = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["x-a"] = "1",
                    ["x-b"] = "2",
                },
            };
            var restored = serializer.Deserialize(serializer.Serialize(original));
            return EnvelopesEqual(original, restored);
        }));
    }

    [Fact]
    public void Protect_open_round_trip()
    {
        Check.QuickThrowOnFailure(Prop.ForAll<(string, byte[])>(t =>
        {
            var (messageType, body) = t;
            var security = Verifier();
            var original = new Envelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = messageType,
                Body = body,
                SentAt = DateTimeOffset.UtcNow,
                PartitionKey = "p",
            };
            var opened = security.OpenInbound(security.ProtectOutbound(original, "property"));
            return opened.MessageType == messageType
                && opened.Body.Span.SequenceEqual(body)
                && opened.Header("avtobus-signature") is not null;
        }));
    }

    [Fact]
    public void Any_single_byte_body_mutation_breaks_signature()
    {
        Check.QuickThrowOnFailure(Prop.ForAll<(byte[], int)>(t =>
        {
            var (body, index) = t;
            if (body.Length == 0)
                return true;
            var security = Verifier();
            var signed = security.ProtectOutbound(new Envelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = "tamper.probe",
                Body = body,
                SentAt = DateTimeOffset.UtcNow,
            }, "property");
            var mutated = signed.Body.ToArray();
            mutated[Math.Abs(index) % mutated.Length] ^= 0xFF;
            var tampered = signed with { Body = mutated };
            try
            {
                security.OpenInbound(tampered);
                return false;
            }
            catch (SecurityViolationException)
            {
                return true;
            }
        }));
    }

    [Fact]
    public void MessagePack_contract_round_trip()
    {
        Check.QuickThrowOnFailure(Prop.ForAll<(string, int)>(t =>
        {
            var (orderId, total) = t;
            var serializer = new AvtoBus.Serialization.MessagePack.MessagePackBusSerializer();
            var original = new PropertyOrder { OrderId = orderId, Total = total };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(writer, original, typeof(PropertyOrder));
            var restored = (PropertyOrder?)serializer.Deserialize(writer.WrittenMemory, typeof(PropertyOrder));
            return restored is not null && restored.OrderId == orderId && restored.Total == total;
        }));
    }

    [Fact]
    public void Protobuf_wellknown_round_trip()
    {
        Check.QuickThrowOnFailure(Prop.ForAll<string>(value =>
        {
            var serializer = new AvtoBus.Serialization.Protobuf.ProtobufBusSerializer();
            var original = new StringValue { Value = value };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(writer, original, typeof(StringValue));
            var restored = (StringValue?)serializer.Deserialize(writer.WrittenMemory, typeof(StringValue));
            return restored is not null && restored.Value == value;
        }));
    }

    private static bool EnvelopesEqual(Envelope a, Envelope b)
        => a.MessageId == b.MessageId
            && a.CorrelationId == b.CorrelationId
            && a.CausationId == b.CausationId
            && a.MessageType == b.MessageType
            && a.Body.Span.SequenceEqual(b.Body.Span)
            && a.ContentType == b.ContentType
            && a.SentAt == b.SentAt
            && a.DeliverAt == b.DeliverAt
            && a.TimeToLive == b.TimeToLive
            && a.PartitionKey == b.PartitionKey
            && a.TenantId == b.TenantId
            && a.Priority == b.Priority
            && a.ReplyTo == b.ReplyTo
            && a.DeliveryAttempt == b.DeliveryAttempt
            && a.TraceParent == b.TraceParent
            && a.Headers.Count == b.Headers.Count
            && a.Headers.All(kv => b.Headers.TryGetValue(kv.Key, out var v) && v == kv.Value);

    [MessagePack.MessagePackObject]
    public sealed class PropertyOrder
    {
        [MessagePack.Key(0)]
        public string OrderId { get; set; } = "";
        [MessagePack.Key(1)]
        public int Total { get; set; }
    }
}

file static class SecurityOptionsExtensions
{
    public static SecurityOptions WithKeys(this SecurityOptions options, SecurityKeys keys)
    {
        options.UseKeys(keys);
        return options;
    }
}
