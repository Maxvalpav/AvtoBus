using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// HMAC-SHA256 подпись конверта. Подписываются стабильные поля конверта и тело —
/// так подмена любого поля или тела сломает проверку на принимающей стороне (идея 451).
/// </summary>
internal static class EnvelopeSigner
{
    /// <summary>Заголовок с подписью (Base64).</summary>
    public const string SignatureHeader = "avtobus-signature";

    /// <summary>Заголовок с идентичностью подписанта.</summary>
    public const string SignedByHeader = "avtobus-signed-by";

    public static string ComputeSignature(Envelope envelope, ReadOnlySpan<byte> key)
    {
        ValidateKeyLength(key);
        using var hmac = new HMACSHA256(key.ToArray());
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("N")));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageType!));
        AddField(hmac, envelope.Body.Span);
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.ContentType!));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.TenantId ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.CorrelationId?.ToString() ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.Header(BusHeaders.User) ?? ""));

        hmac.TransformFinalBlock([], 0, 0);
        return Convert.ToBase64String(hmac.Hash ?? []);
    }

    private static void ValidateKeyLength(ReadOnlySpan<byte> key)
    {
        if (key.Length != 32)
            throw new ArgumentException($"Signing key must be 32 bytes, got {key.Length}", nameof(key));
    }

    public static bool Verify(Envelope envelope, ReadOnlySpan<byte> key)
    {
        var expected = ComputeSignature(envelope, key);
        var actual = envelope.Header(SignatureHeader);
        if (actual is null) return false;
        try
        {
            return CryptographicOperations.FixedTimeEquals(
                Convert.FromBase64String(actual),
                Convert.FromBase64String(expected));
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static void AddField(HMACSHA256 hmac, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, value.Length);
        hmac.TransformBlock(length.ToArray(), 0, 4, null, 0);
        if (value.Length > 0)
        {
            // Avoid extra alloc for small values via ArrayPool
            var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(value.Length);
            try
            {
                value.CopyTo(rented);
                hmac.TransformBlock(rented, 0, value.Length, null, 0);
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
        }
    }
}
