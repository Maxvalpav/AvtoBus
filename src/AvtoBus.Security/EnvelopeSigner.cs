using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// HMAC-SHA256 подпись конверта. Схема v2 подписывает стабильные поля маршрутизации
/// (ReplyTo, PartitionKey, Priority, DeliverAt, TTL, TraceParent, CausationId) —
/// подмена любого из них ломает проверку (идея 451).
///
/// Не подписываются поля, мутирующие при легитимной транспортировке:
/// DeliveryAttempt (растёт на ретраях), SentAt, Hops, exception-заголовки DLQ,
/// сами заголовки подписи. Кастомные заголовки тоже вне покрытия осознанно:
/// критичные для целостности данные приложение кладёт в тело (оно подписано).
/// </summary>
internal static class EnvelopeSigner
{
    /// <summary>Заголовок с подписью (Base64).</summary>
    public const string SignatureHeader = "avtobus-signature";

    /// <summary>Заголовок с идентичностью подписанта.</summary>
    public const string SignedByHeader = "avtobus-signed-by";

    /// <summary>Версия схемы подписи. v1 покрывала только базу; v2 — и маршрутизацию.</summary>
    public const string SignatureVersionHeader = "avtobus-sig-version";

    public const int V1 = 1;
    public const int V2 = 2;

    public static string ComputeSignature(Envelope envelope, ReadOnlySpan<byte> key, int version = V2)
    {
        ValidateKeyLength(key);
        // HMACSHA256 копирует ключ внутрь; нашу копию затираем сразу после использования,
        // чтобы секрет не оставался в управляемой куче.
        var keyCopy = key.ToArray();
        try
        {
            using var hmac = new HMACSHA256(keyCopy);
            if (version >= V2)
                AddV2Fields(hmac, envelope);
            else
                AddV1Fields(hmac, envelope);

            hmac.TransformFinalBlock([], 0, 0);
            return Convert.ToBase64String(hmac.Hash ?? []);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyCopy);
        }
    }

    private static void AddV1Fields(HMACSHA256 hmac, Envelope envelope)
    {
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("N")));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.MessageType!));
        AddField(hmac, envelope.Body.Span);
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.ContentType!));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.TenantId ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.CorrelationId?.ToString() ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.Header(BusHeaders.User) ?? ""));
    }

    private static void AddV2Fields(HMACSHA256 hmac, Envelope envelope)
    {
        AddV1Fields(hmac, envelope);
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.CausationId?.ToString() ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.ReplyTo ?? ""));
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.PartitionKey ?? ""));
        Span<byte> num = stackalloc byte[8];
        BinaryPrimitives.WriteInt32BigEndian(num[..4], envelope.Priority);
        hmac.TransformBlock(num[..4].ToArray(), 0, 4, null, 0);
        BinaryPrimitives.WriteInt64BigEndian(num, envelope.DeliverAt?.UtcTicks ?? 0);
        hmac.TransformBlock(num.ToArray(), 0, 8, null, 0);
        BinaryPrimitives.WriteInt64BigEndian(num, envelope.TimeToLive?.Ticks ?? 0);
        hmac.TransformBlock(num.ToArray(), 0, 8, null, 0);
        AddField(hmac, Encoding.UTF8.GetBytes(envelope.TraceParent ?? ""));
    }

    private static void ValidateKeyLength(ReadOnlySpan<byte> key)
    {
        if (key.Length != 32)
            throw new ArgumentException($"Signing key must be 32 bytes, got {key.Length}", nameof(key));
    }

    public static bool Verify(Envelope envelope, ReadOnlySpan<byte> key)
    {
        // Версия читается из заголовка: v2-сообщение без заголовка (заголовок стёрт)
        // упадёт на v1-проверке — подмена версии не даёт пройти проверку.
        var version = envelope.Header(SignatureVersionHeader) == "2" ? V2 : V1;
        string expected;
        try
        {
            expected = ComputeSignature(envelope, key, version);
        }
        catch (ArgumentException)
        {
            return false;
        }
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
            // Avoid extra alloc for small values via ArrayPool; буфер затираем при
            // возврате, чтобы фрагменты подписанных данных не оставались в пуле.
            var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(value.Length);
            try
            {
                value.CopyTo(rented);
                hmac.TransformBlock(rented, 0, value.Length, null, 0);
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented, clearArray: true); }
        }
    }
}
