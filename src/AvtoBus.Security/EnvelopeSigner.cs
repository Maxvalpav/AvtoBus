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
        using var hmac = new HMACSHA256(key.ToArray());
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("N")));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageType));
        AddField(hmac, envelope.Body.Span);
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.ContentType));

        // Заголовок пользователя (идея 454) закреплён подписью: подмена «от чьего имени»
        // сломает проверку. Отсутствие заголовка тоже подписано (пустое значение).
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.Header(BusHeaders.User) ?? ""));

        hmac.TransformFinalBlock([], 0, 0);
        return Convert.ToBase64String(hmac.Hash ?? []);
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
        var length = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, value.Length);
        hmac.TransformBlock(length, 0, length.Length, null, 0);
        hmac.TransformBlock(value.ToArray(), 0, value.Length, null, 0);
    }
}
