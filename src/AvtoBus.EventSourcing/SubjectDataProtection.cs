using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AvtoBus.EventSourcing;

/// <summary>
/// Конфигуратор crypto-shredding (идея 264): какие поля события шифруются per-subject.
/// Поле-субъект (ключ шифрования) остаётся открытым — это индекс для GDPR-отчёта.
/// </summary>
public sealed class SubjectEncryptionConfigurator
{
    private readonly Dictionary<string, SubjectEventConfig> _configs = new();

    public IReadOnlyDictionary<string, SubjectEventConfig> Configs => _configs;

    /// <summary>
    /// Регистрирует событие: поле <paramref name="subjectIdSelector"/> — идентификатор субъекта
    /// (остаётся открытым, является ключом), а перечисленные поля шифруются.
    /// </summary>
    public SubjectEncryptionConfigurator PerSubject<TEvent>(
        Expression<Func<TEvent, object>> subjectIdSelector,
        params Expression<Func<TEvent, object?>>[] fields)
        where TEvent : class
    {
        var eventType = MessageTypeNaming.NameOf(typeof(TEvent));
        var subjectId = PropertyName(subjectIdSelector);
        var fieldNames = fields.Select(PropertyName).ToArray();

        if (!fieldNames.Contains(subjectId, StringComparer.Ordinal))
            fieldNames = fieldNames.Append(subjectId).ToArray();

        _configs[eventType] = new SubjectEventConfig(eventType, subjectId, fieldNames);
        return this;
    }

    private static string PropertyName<TDelegate>(Expression<TDelegate> expression)
    {
        var body = expression.Body;

        // e => e.UserId.ToString() → распаковываем в UserId.
        if (body is MethodCallExpression call && call.Method.Name == "ToString"
            && call.Object is MemberExpression toStringMember)
            body = toStringMember;

        var member = body as MemberExpression;
        if (member is null && body is UnaryExpression unary)
            member = unary.Operand as MemberExpression;

        return member?.Member.Name ?? throw new ArgumentException(
            "Subject selector must be a property access expression (e.g. e => e.UserId)", nameof(expression));
    }
}

public sealed record SubjectEventConfig(string EventType, string SubjectIdField, IReadOnlyList<string> EncryptedFields);

/// <summary>
/// Хранилище ключей субъектов: случайный AES-256 ключ на субъекта.
/// «Право на забвение» = удаление ключа — события остаются, но PII нечитаемо (идея 264).
/// </summary>
public interface ISubjectKeyRing
{
    bool TryGetKey(string subjectId, out byte[] key);

    byte[] GetOrCreateKey(string subjectId);

    /// <summary>Удаляет ключ субъекта — данные становятся нечитаемыми.</summary>
    void Forget(string subjectId);

    bool IsForgotten(string subjectId);
}

/// <summary>In-memory кольцо ключей. Для durable-режима подставьте реализацию на БД/внешнем KMS.</summary>
public sealed class InMemorySubjectKeyRing : ISubjectKeyRing
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _keys = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _forgotten = new();

    public bool TryGetKey(string subjectId, out byte[] key) => _keys.TryGetValue(subjectId, out key!);

    public byte[] GetOrCreateKey(string subjectId)
        => _keys.GetOrAdd(subjectId, _ => System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));

    public void Forget(string subjectId) { _keys.TryRemove(subjectId, out _); _forgotten[subjectId] = 1; }

    public bool IsForgotten(string subjectId) => _forgotten.ContainsKey(subjectId);
}

/// <summary>
/// Crypto-shredding поверх JSON-представления события: значения зашифрованных полей заменяются
/// на <c>{"$enc": base64, "k": subjectId}</c>. Дешифровка возможна, пока ключ субъекта существует.
/// </summary>
public sealed class SubjectDataProtection
{
    private const string Marker = "$enc";
    private const string KeyField = "k";

    private readonly ISubjectKeyRing _keys;
    private readonly IReadOnlyDictionary<string, SubjectEventConfig> _configs;

    public SubjectDataProtection(
        ISubjectKeyRing keys,
        SubjectEncryptionConfigurator configurator)
    {
        _keys = keys;
        _configs = configurator.Configs;
    }

    /// <summary>Шифрует поля события по конфигурации. Возвращает JSON-байты.</summary>
    public ReadOnlyMemory<byte> Protect(object payload, string eventType)
    {
        if (!_configs.TryGetValue(eventType, out var config))
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(payload, payload.GetType());

        var node = System.Text.Json.JsonSerializer.SerializeToNode(payload, payload.GetType());
        if (node is null)
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(payload, payload.GetType());

        var subjectId = (node[config.SubjectIdField] as JsonValue)?.ToString()
                        ?? throw new InvalidOperationException(
                            $"Subject field '{config.SubjectIdField}' is missing on event '{eventType}'");

        var key = _keys.GetOrCreateKey(subjectId);

        foreach (var field in config.EncryptedFields)
        {
            if (field == config.SubjectIdField)
                continue;

            if (node[field] is { } fieldNode)
                node[field] = EncryptField(fieldNode, subjectId, key);
        }

        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(node);
    }

    /// <summary>
    /// Расшифровывает поля события. Если ключ субъекта удалён — зашифрованные поля возвращаются
    /// как <c>null</c> (данные «забыты», событие остаётся в сторе).
    /// </summary>
    public object Unprotect(ReadOnlyMemory<byte> data, string eventType, Type clrType)
    {
        if (!_configs.TryGetValue(eventType, out var config))
            return System.Text.Json.JsonSerializer.Deserialize(data.Span, clrType)!;

        var node = JsonNode.Parse(data.Span);
        if (node is null)
            return System.Text.Json.JsonSerializer.Deserialize(data.Span, clrType)!;

        foreach (var field in config.EncryptedFields)
        {
            if (field == config.SubjectIdField)
                continue;

            if (node[field] is { } fieldNode)
                node[field] = DecryptField(fieldNode);
        }

        return node.Deserialize(clrType)!;
    }

    /// <summary>Извлекает subjectId из события (для GDPR-отчёта), не требуя ключа.</summary>
    public bool TryGetSubjectId(string eventType, ReadOnlyMemory<byte> data, out string? subjectId)
    {
        subjectId = null;
        if (!_configs.TryGetValue(eventType, out var config))
            return false;

        var node = JsonNode.Parse(data.Span);
        subjectId = (node?[config.SubjectIdField] as JsonValue)?.ToString();
        return subjectId is not null;
    }

    private static JsonObject EncryptField(JsonNode fieldNode, string subjectId, byte[] key)
    {
        var plaintext = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(fieldNode);
        var nonce = System.Security.Cryptography.RandomNumberGenerator.GetBytes(12);
        var tag = new byte[16];
        var ciphertext = new byte[plaintext.Length];

        using var aes = new System.Security.Cryptography.AesGcm(key, tag.Length);
        aes.Encrypt(nonce, plaintext, ciphertext, tag, null);

        // Payload: [nonce(12) | ciphertext | tag(16)]
        var payload = new byte[12 + ciphertext.Length + tag.Length];
        nonce.CopyTo(payload, 0);
        ciphertext.CopyTo(payload, 12);
        tag.CopyTo(payload, 12 + ciphertext.Length);

        return new JsonObject
        {
            [Marker] = Convert.ToBase64String(payload),
            [KeyField] = subjectId,
        };
    }

    private JsonNode? DecryptField(JsonNode fieldNode)
    {
        if (fieldNode is not JsonObject obj
            || obj[Marker] is not { } markerValue
            || obj[KeyField] is not { } keyValue)
            return fieldNode;

        var subjectId = keyValue.ToString();
        if (!_keys.TryGetKey(subjectId, out var key))
            return null; // ключ забыт → данные нечитаемы

        try
        {
            return DecryptCore(Convert.FromBase64String(markerValue.ToString()), key);
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            return null;
        }
    }

    private static JsonNode? DecryptCore(byte[] payload, byte[] key)
    {
        const int tagLength = 16;
        var nonce = payload.AsSpan(0, 12).ToArray();
        var raw = new byte[payload.Length - 12 - tagLength];

        using var aes = new System.Security.Cryptography.AesGcm(key, tagLength);
        aes.Decrypt(nonce, payload.AsSpan(12, raw.Length), payload.AsSpan(12 + raw.Length), raw, null);

        return JsonNode.Parse(raw);
    }
}
