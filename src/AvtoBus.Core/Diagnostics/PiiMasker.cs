using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AvtoBus.Contracts;

namespace AvtoBus.Diagnostics;

/// <summary>
/// Маскирование персональных данных в диагностических выводах (идея 456).
/// поля, помеченные <see cref="PersonalDataAttribute"/>, заменяются на детерминированную
/// маску: в отличие от «***», одинаковая исходная строка даёт одинаковую маску —
/// по маскированному выводу можно коррелировать записи, не раскрывая сами данные.
/// </summary>
/// <remarks>
/// Диагностический путь (вторая линия обороны, DLQ-описания) использует reflection-STJ —
/// ограниченный legacy-режим (док 01 §codegen). Под AOT выключен (PiiMaskingEnabled=false).
/// </remarks>
public static class PiiMasker
{
    private const int MaxCachedTypes = 2000;

    /// <summary>
    /// Соль детерминированной маски (pepper развёртки). По умолчанию константа —
    /// маски коррелируются между процессами и рестартами; оператор задаёт свою через
    /// <c>BusOptions.PiiMaskSalt</c> (утечка соли + логов = брутфорс коротких PII).
    /// </summary>
    public static string Salt { get; set; } = "avtobus-pii-v2";

    private static readonly ConcurrentDictionary<Type, FrozenDictionary<string, string>> MaskedProperties =
        new();

    /// <summary>
    /// Маскирует строку: HMAC-подобная конструкция SHA256(salt || value), 128 бит.
    /// Детерминирована для корреляции в логах; короткие PII принципиально брутфорсятся
    /// при известной соли — поэтому соль развёртки должна быть секретом (см. Salt).
    /// </summary>
    public static string Mask(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return value ?? "";

        var saltBytes = Encoding.UTF8.GetBytes(Salt);
        var valueBytes = Encoding.UTF8.GetBytes(value);
        var input = new byte[saltBytes.Length + valueBytes.Length];
        Buffer.BlockCopy(saltBytes, 0, input, 0, saltBytes.Length);
        Buffer.BlockCopy(valueBytes, 0, input, saltBytes.Length, valueBytes.Length);
        try
        {
            var hash = System.Security.Cryptography.SHA256.HashData(input);
            // 16 байт = 32 hex = 128 бит: brute-force коротких PII упирается в перебор входов, а не в коллизии.
            return $"###{Convert.ToHexString(hash, 0, 16).ToLowerInvariant()}";
        }
        finally
        {
            CryptographicOperations.ZeroMemory(input);
        }
    }

    /// <summary>
    /// Маскированный вывод контракта: PII-поля заменены масками, остальное сохранено.
    /// Выход стабилен для логов: одинаковый контракт всегда печатается одинаково.
    /// </summary>
    [RequiresUnreferencedCode(
        "Маскирование контрактов использует reflection-STJ и сканирование атрибутов. " +
        "Диагностический путь (legacy): под AOT PiiMaskingEnabled должен быть выключен.")]
    public static string ToMaskedText(object? message)
    {
        if (message is null)
            return "null";

        var type = message.GetType();
        if (MaskedProperties.Count >= MaxCachedTypes && !MaskedProperties.ContainsKey(type))
            return "***pii-redacted***";
        var fields = MaskedProperties.GetOrAdd(type, BuildFieldMap);
        if (fields.Count == 0)
            return JsonSerializer.Serialize(message, type);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(message, type));
        var masked = MaskElement(document.RootElement, fields);
        return JsonSerializer.Serialize(masked, new JsonSerializerOptions { WriteIndented = false });
    }

    private static object? MaskElement(JsonElement element, FrozenDictionary<string, string> fields)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(
                p => p.Name,
                p => fields.ContainsKey(p.Name) && p.Value.ValueKind is not JsonValueKind.Null
                    ? Mask(p.Value.GetString() ?? p.Value.GetRawText())
                    : MaskElement(p.Value, fields),
                StringComparer.Ordinal),
            JsonValueKind.Array => element.EnumerateArray().Select(e => MaskElement(e, fields)).ToArray(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetDecimal(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private static object? Read(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.GetDecimal(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Array => element.EnumerateArray().Select(Read).ToArray(),
        JsonValueKind.Object => element.EnumerateObject().ToDictionary(
            p => p.Name,
            p => Read(p.Value),
            StringComparer.Ordinal),
        _ => element.GetRawText(),
    };

    [RequiresUnreferencedCode("Сканирование свойств на PersonalDataAttribute — reflection (legacy).")]
    private static FrozenDictionary<string, string> BuildFieldMap(Type type)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var contractMarked = type.GetCustomAttribute<PersonalDataAttribute>() is not null;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var attribute = property.GetCustomAttribute<PersonalDataAttribute>();
            if (contractMarked || attribute is not null)
            {
                var name = property.Name;
                if (string.IsNullOrEmpty(name)) continue;
                map[name] = attribute?.Category ?? "pii";
                // Также camelCase вариант, т.к. JsonSerializer может использовать PropertyNamingPolicy.CamelCase
                if (name.Length > 1)
                {
                    var camel = char.ToLowerInvariant(name[0]) + name[1..];
                    map.TryAdd(camel, attribute?.Category ?? "pii");
                }
                else
                {
                    map.TryAdd(name.ToLowerInvariant(), attribute?.Category ?? "pii");
                }
            }
        }

        return map.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }
}
