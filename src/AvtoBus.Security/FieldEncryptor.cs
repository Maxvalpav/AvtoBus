using System.Reflection;
using System.Security.Cryptography;

namespace AvtoBus.Security;

/// <summary>
/// Per-field шифрование string-полей с [Encrypted] (идея 455): каждое поле — свой nonce, ключ — из KeyRing.
/// Формат на проводе: <c>enc:BASE64_NONCE:BASE64_CT</c>. Не шифрует null/пустые.
/// </summary>
public static class FieldEncryptor
{
    private const string Prefix = "enc:";

    public static void EncryptFields(object message, ReadOnlySpan<byte> key)
    {
        foreach (var prop in GetEncryptedProperties(message.GetType()))
        {
            if (prop.GetValue(message) is not string plain || string.IsNullOrEmpty(plain))
                continue;
            var cipher = EncryptString(plain, key);
            prop.SetValue(message, cipher);
        }
    }

    public static void DecryptFields(object message, ReadOnlySpan<byte> key)
    {
        foreach (var prop in GetEncryptedProperties(message.GetType()))
        {
            if (prop.GetValue(message) is not string cipher || !cipher.StartsWith(Prefix, StringComparison.Ordinal))
                continue;
            var plain = DecryptString(cipher, key);
            prop.SetValue(message, plain);
        }
    }

    private static string EncryptString(string plain, ReadOnlySpan<byte> key)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(plain);
        var nonce = RandomNumberGenerator.GetBytes(12);
        var tag = new byte[16];
        var ct = new byte[bytes.Length];
        using var aes = new AesGcm(key, tag.Length);
        aes.Encrypt(nonce, bytes, ct, tag, null);
        var payload = new byte[ct.Length + tag.Length];
        ct.CopyTo(payload, 0);
        tag.CopyTo(payload, ct.Length);
        return $"{Prefix}{Convert.ToBase64String(nonce)}:{Convert.ToBase64String(payload)}";
    }

    private static string DecryptString(string cipher, ReadOnlySpan<byte> key)
    {
        var parts = cipher.Split(':', 3);
        if (parts.Length != 3) throw new SecurityViolationException($"Malformed encrypted field: {cipher}");
        byte[] nonce, payload;
        try { nonce = Convert.FromBase64String(parts[1]); } catch (FormatException ex) { throw new SecurityViolationException("Invalid nonce", ex); }
        try { payload = Convert.FromBase64String(parts[2]); } catch (FormatException ex) { throw new SecurityViolationException("Invalid payload", ex); }
        try
        {
            var plain = BodyEncryptor.Decrypt(payload, key, nonce);
            return System.Text.Encoding.UTF8.GetString(plain.Span);
        }
        catch (Exception ex) when (ex is CryptographicException or ArgumentException)
        {
            throw new SecurityViolationException("Field decryption failed", ex);
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, PropertyInfo[]> Cache = new();

    private static IEnumerable<PropertyInfo> GetEncryptedProperties(Type type)
        => Cache.GetOrAdd(type, t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
               .Where(p => p.CanRead && p.CanWrite && p.GetCustomAttribute<EncryptedAttribute>(inherit: true) is not null && p.PropertyType == typeof(string))
               .ToArray());
}
