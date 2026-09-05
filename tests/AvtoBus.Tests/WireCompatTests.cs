using System.Text.Json;
using AvtoBus.Security;

namespace AvtoBus.Tests;

/// <summary>
/// Golden-тесты wire-совместимости (E-03): сериализованные конверты прошлых версий
/// лежат в <c>tests/wire-fixtures/</c> и обязаны читаться текущим кодом.
/// Фикстуры сгенерированы одноразовым генератором (см. meta внутри JSON);
/// ключ — тестовый, никогда не продовый.
/// </summary>
public class WireCompatTests
{
    private static string FixturePath(string name)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "wire-fixtures", name);
            if (File.Exists(candidate))
                return candidate;
            if (File.Exists(Path.Combine(dir.FullName, "AvtoBus.slnx")))
                break;
            dir = dir.Parent;
        }

        throw new FileNotFoundException($"Wire fixture not found: {name}");
    }

    private static Envelope LoadFixture(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath(name)));
        var root = doc.RootElement;
        Guid? OptGuid(string prop)
            => root.TryGetProperty(prop, out var el) && el.ValueKind == JsonValueKind.String
                ? Guid.Parse(el.GetString()!)
                : null;

        return new Envelope
        {
            MessageId = Guid.Parse(root.GetProperty("messageId").GetString()!),
            CorrelationId = OptGuid("correlationId"),
            CausationId = OptGuid("causationId"),
            MessageType = root.GetProperty("messageType").GetString()!,
            Body = Convert.FromBase64String(root.GetProperty("bodyBase64").GetString()!),
            ContentType = root.GetProperty("contentType").GetString()!,
            SentAt = DateTimeOffset.FromUnixTimeSeconds(long.Parse(root.GetProperty("sentAtUnixSeconds").GetString()!)),
            DeliverAt = OptUnix("deliverAtUnixSeconds") is { } d ? DateTimeOffset.FromUnixTimeSeconds(d) : null,
            TimeToLive = root.TryGetProperty("ttlSeconds", out var ttl) && ttl.ValueKind == JsonValueKind.String
                ? TimeSpan.FromSeconds(double.Parse(ttl.GetString()!, System.Globalization.CultureInfo.InvariantCulture))
                : null,
            PartitionKey = OptString("partitionKey"),
            TenantId = OptString("tenantId"),
            Priority = int.Parse(root.GetProperty("priority").GetString()!),
            ReplyTo = OptString("replyTo"),
            TraceParent = OptString("traceParent"),
            Headers = root.GetProperty("headers").EnumerateObject()
                .ToDictionary(p => p.Name, p => p.Value.GetString()!, StringComparer.Ordinal),
        };

        long? OptUnix(string prop)
            => root.TryGetProperty(prop, out var el) && el.ValueKind == JsonValueKind.String
                ? long.Parse(el.GetString()!)
                : null;

        string? OptString(string prop)
            => root.TryGetProperty(prop, out var el) && el.ValueKind == JsonValueKind.String
                ? el.GetString()
                : null;
    }

    private static byte[] FixtureKey(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath(name)));
        return Convert.FromHexString(doc.RootElement.GetProperty("meta").GetProperty("signingKeyHex").GetString()!);
    }

    private static EnvelopeSecurity Verifier(string fixture, int minimumVersion = 2)
    {
        var key = FixtureKey(fixture);
        var options = new SecurityOptions
        {
            RequireSignature = true,
            MinimumSignatureVersion = minimumVersion,
            // Фикстуры старые по построению: окно возраста расширяем, чтобы проверять
            // именно подпись и политику версий, а не anti-replay (у него свои тесты).
            MaxSignatureAge = TimeSpan.FromDays(365 * 100),
        };
        options.UseKeys(new SecurityKeys
        {
            SigningKey = key,
            EncryptionKey = new byte[32],
        });
        return new EnvelopeSecurity(options);
    }

    [Theory]
    [InlineData("envelope-v2.json")]
    [InlineData("envelope-v3.json")]
    public void Supported_versions_open_cleanly(string fixture)
    {
        var opened = Verifier(fixture).OpenInbound(LoadFixture(fixture));
        Assert.Equal("wire-compat.order-placed", opened.MessageType);
        Assert.Equal("""{"orderId":"42","total":100}""", System.Text.Encoding.UTF8.GetString(opened.Body.Span));
        Assert.Equal("orders", opened.PartitionKey);
        Assert.Equal("tester", opened.Header("avtobus-user"));
        Assert.NotNull(opened.Header("avtobus-signature"));
    }

    [Fact]
    public void V1_rejected_by_default_minimum_version()
    {
        Assert.Throws<SecurityViolationException>(() =>
            Verifier("envelope-v1.json").OpenInbound(LoadFixture("envelope-v1.json")));
    }

    [Fact]
    public void V1_accepted_with_minimum_1_rollout()
    {
        var opened = Verifier("envelope-v1.json", minimumVersion: 1).OpenInbound(LoadFixture("envelope-v1.json"));
        Assert.Equal("wire-compat.order-placed", opened.MessageType);
    }

    [Fact]
    public void Mutated_body_breaks_signature()
    {
        var envelope = LoadFixture("envelope-v3.json");
        var body = envelope.Body.ToArray();
        body[^1] ^= 0xFF;
        var tampered = envelope with { Body = body };
        Assert.Throws<SecurityViolationException>(() => Verifier("envelope-v3.json").OpenInbound(tampered));
    }

    [Fact]
    public void Mutated_routing_field_breaks_v2plus_signature()
    {
        var tampered = LoadFixture("envelope-v3.json") with { PartitionKey = "hacked" };
        Assert.Throws<SecurityViolationException>(() => Verifier("envelope-v3.json").OpenInbound(tampered));
    }

    [Fact]
    public void Unsigned_custom_header_mutation_keeps_valid()
    {
        // Кастомные заголовки вне покрытия подписи осознанно: критичные данные — в теле.
        var tampered = LoadFixture("envelope-v3.json").WithHeader("x-note", "changed");
        var opened = Verifier("envelope-v3.json").OpenInbound(tampered);
        Assert.Equal("changed", opened.Header("x-note"));
    }
}
