using System.Buffers.Binary;
using System.Security.Cryptography;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// AES-256-GCM шифрование тела конверта (идея 455). Не имеет аутентификации само по себе —
/// нонс и ciphertext подписываются HMAC'ом вместе с остальным конвертом (см. EnvelopeSigner),
/// так что целостность гарантируется на уровне подписи.
/// </summary>
internal static class BodyEncryptor
{
    /// <summary>Заголовок: нонс (Base64), 12 байт для GCM.</summary>
    public const string NonceHeader = "avtobus-encryption-nonce";

    public static bool IsEncrypted(Envelope envelope) => envelope.Header(NonceHeader) is not null;

    public static ReadOnlyMemory<byte> Encrypt(ReadOnlySpan<byte> body, ReadOnlySpan<byte> key, out string nonceBase64)
    {
        if (key.Length != 32) throw new ArgumentException($"Encryption key must be 32 bytes, got {key.Length}", nameof(key));
        var nonce = RandomNumberGenerator.GetBytes(12);
        var tag = new byte[16];
        var ciphertext = new byte[body.Length];

        using (var aes = new AesGcm(key, AesGcm.TagByteSizes.MaxSize))
        {
            aes.Encrypt(nonce, body, ciphertext, tag, null);
        }

        var payload = new byte[ciphertext.Length + tag.Length];
        ciphertext.CopyTo(payload, 0);
        tag.CopyTo(payload, ciphertext.Length);

        nonceBase64 = Convert.ToBase64String(nonce);
        return payload;
    }

    public static ReadOnlyMemory<byte> Decrypt(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce)
    {
        if (key.Length != 32) throw new ArgumentException($"Encryption key must be 32 bytes, got {key.Length}", nameof(key));
        if (nonce.Length != 12) throw new ArgumentException($"Nonce must be 12 bytes, got {nonce.Length}", nameof(nonce));
        const int tagLength = 16;
        if (payload.Length < tagLength) throw new CryptographicException("Ciphertext too short");
        var plaintextLength = payload.Length - tagLength;
        var raw = new byte[plaintextLength];

        using var aes = new AesGcm(key, AesGcm.TagByteSizes.MaxSize);
        aes.Decrypt(nonce, payload[..^tagLength], payload[^tagLength..], raw, null);

        return raw;
    }

    public static bool TryReadNonce(Envelope envelope, out byte[] nonce)
    {
        nonce = [];
        var header = envelope.Header(NonceHeader);
        if (header is null)
            return false;
        try
        {
            nonce = Convert.FromBase64String(header);
            return nonce.Length == 12;
        }
        catch (FormatException)
        {
            return false;
        }
    }
}
