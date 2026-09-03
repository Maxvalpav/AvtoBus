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
    /// <summary>
    /// Соль детерминированной маски (pepper развёртки). По умолчанию константа —
    /// маски коррелируются между процессами и рестартами; оператор задаёт свою через
    /// <c>BusOptions.PiiMaskSalt</c> (утечка соли + логов = брутфорс коротких PII).
    /// </summary>
    public static string Salt { get; set; } = "avtobus-pii-v2";

    private static readonly ConcurrentDictionary<Type, FrozenDictionary<string, FieldRule>> MaskedProperties =
        new();

    /// <summary>Правило поля: категория PII (null — не PII) + CLR-тип значения для рекурсии.</summary>
    private sealed record FieldRule(string? Category, Type? ValueType);

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
        // Без лимита числа типов: раньше при 2000+ типов новые навсегда возвращали
        // заглушку и диагностика слепла. Карт — по одной на CLR-тип, в приложениях их сотни.
        var fields = MaskedProperties.GetOrAdd(type, BuildFieldMap);
        if (fields.Count == 0)
            return JsonSerializer.Serialize(message, type);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(message, type));
        var masked = MaskElement(document.RootElement, type);
        return JsonSerializer.Serialize(masked, new JsonSerializerOptions { WriteIndented = false });
    }

    private static object? MaskElement(JsonElement element, Type? dotnetType)
    {
        var fields = dotnetType is null ? null : MaskedProperties.GetOrAdd(dotnetType, BuildFieldMap);
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(
                p => p.Name,
                p =>
                {
                    FieldRule? rule = null;
                    fields?.TryGetValue(p.Name, out rule);
                    if (rule?.Category is not null && p.Value.ValueKind is not JsonValueKind.Null)
                        return Mask(p.Value.GetString() ?? p.Value.GetRawText());
                    // Рекурсия с CLR-типом свойства: одноимённое поле вложенного типа
                    // без атрибута больше не маскируется ложно родительской картой.
                    return MaskElement(p.Value, rule?.ValueType ?? ElementTypeOf(dotnetType));
                },
                StringComparer.Ordinal),
            JsonValueKind.Array => element.EnumerateArray()
                .Select(e => MaskElement(e, ElementTypeOf(dotnetType))).ToArray(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetDecimal(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    /// <summary>Тип элементов массива/списка для рекурсии; иначе null.</summary>
    private static Type? ElementTypeOf(Type? type)
    {
        if (type is null) return null;
        if (type.IsArray) return type.GetElementType();
        if (type.IsGenericType)
        {
            var args = type.GetGenericArguments();
            if (args.Length == 1) return args[0];
        }
        return null;
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
    private static FrozenDictionary<string, FieldRule> BuildFieldMap(Type type)
    {
        var map = new Dictionary<string, FieldRule>(StringComparer.OrdinalIgnoreCase);
        var contractMarked = type.GetCustomAttribute<PersonalDataAttribute>() is not null;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var attribute = property.GetCustomAttribute<PersonalDataAttribute>();
            if (contractMarked || attribute is not null)
            {
                var name = property.Name;
                if (string.IsNullOrEmpty(name)) continue;
                var rule = new FieldRule(attribute?.Category ?? "pii", property.PropertyType);
                map[name] = rule;
                // Также camelCase вариант, т.к. JsonSerializer может использовать PropertyNamingPolicy.CamelCase
                if (name.Length > 1)
                {
                    var camel = char.ToLowerInvariant(name[0]) + name[1..];
                    map.TryAdd(camel, rule);
                }
                else
                {
                    map.TryAdd(name.ToLowerInvariant(), rule);
                }
            }
        }

        return map.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }
}
