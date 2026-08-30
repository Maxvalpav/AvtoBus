using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
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
    private static readonly ConcurrentDictionary<Type, FrozenDictionary<string, string>> MaskedProperties =
        new();

    /// <summary>Маскирует строку: короткий хэш исходника вместо полного значения.</summary>
    public static string Mask(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return value ?? "";

        var hash = System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return $"###{Convert.ToHexString(hash, 0, 4).ToLowerInvariant()}";
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
        var fields = MaskedProperties.GetOrAdd(type, BuildFieldMap);
        if (fields.Count == 0)
            return JsonSerializer.Serialize(message, type);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(message, type));
        var output = new Dictionary<string, object?>(StringComparer.Ordinal);

        foreach (var element in document.RootElement.EnumerateObject())
        {
            if (fields.TryGetValue(element.Name, out _) && element.Value.ValueKind is not JsonValueKind.Null)
                output[element.Name] = Mask(element.Value.GetString() ?? element.Value.GetRawText());
            else
                output[element.Name] = Read(element.Value);
        }

        return JsonSerializer.Serialize(
            output,
            new JsonSerializerOptions { WriteIndented = false });
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
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        var contractMarked = type.GetCustomAttribute<PersonalDataAttribute>() is not null;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var attribute = property.GetCustomAttribute<PersonalDataAttribute>();
            if (contractMarked || attribute is not null)
                map[property.Name] = attribute?.Category ?? "pii";
        }

        return map.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
