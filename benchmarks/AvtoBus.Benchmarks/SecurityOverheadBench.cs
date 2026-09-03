using System.Text;
using AvtoBus.Diagnostics;
using AvtoBus.Runtime;
using AvtoBus.Security;
using BenchmarkDotNet.Attributes;

namespace AvtoBus.Benchmarks;

/// <summary>
/// Стоимость защит и маскирования на сообщение (in-memory, без брокеров):
/// HMAC-подпись конверта, unique-хэш тела, PII-маска. Регресс-контроль аллокаций
/// после оптимизаций (IncrementalHash, статические опции, кэш атрибутов).
/// </summary>
[MemoryDiagnoser]
public class SecurityOverheadBench
{
    private Envelope _envelope = null!;
    private EnvelopeSecurity _security = null!;
    private object _message = null!;
    private Type _messageType = null!;

    [GlobalSetup]
    public void Setup()
    {
        _message = new OrderPlaced(Guid.NewGuid(), 42.5m, "USD");
        _messageType = _message.GetType();
        _envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = "orders.order-placed.v1",
            Body = Encoding.UTF8.GetBytes("{\"orderId\":\"x\",\"total\":42.5}"),
            ContentType = "application/json",
            SentAt = DateTimeOffset.UtcNow,
            TenantId = "eu",
        };
        _security = new EnvelopeSecurity(new SecurityOptions
        {
            MasterSecret = "bench-secret",
            RequireSignature = true,
        });
    }

    [Benchmark]
    public string Sign_envelope() => EnvelopeSecurity_Sign();

    private string EnvelopeSecurity_Sign()
        => _security.ProtectOutbound(_envelope, "bench").Header("avtobus-signature")!;

    [Benchmark]
    public bool Verify_envelope()
    {
        var signed = _security.ProtectOutbound(_envelope, "bench");
        return _security.HasValidSignature(signed);
    }

    [Benchmark]
    public string Unique_key_by_args()
        => UniqueKeyComputer.Compute(_message, _messageType, "orders", new UniqueJobAttribute { ByArgs = true });

    [Benchmark]
    public string Pii_mask() => PiiMasker.Mask("user@example.com");
}
