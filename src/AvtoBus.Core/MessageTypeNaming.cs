using System.Collections.Concurrent;
using System.Reflection;
using System.Text;

namespace AvtoBus;

/// <summary>
/// Вычисляет стабильное имя контракта: <c>orders.order-placed.v1</c>.
/// Порядок приоритетов: <see cref="MessageAliasAttribute"/> → <see cref="TopicAttribute"/> → конвенция kebab-case.
/// </summary>
public static class MessageTypeNaming
{
    private static readonly ConcurrentDictionary<Type, string> Names = new();
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<string>> Aliases = new();

    public static string NameOf(Type type) => Names.GetOrAdd(type, static t =>
    {
        if (t.GetCustomAttribute<MessageAliasAttribute>() is { } alias)
            return alias.Name;

        if (t.GetCustomAttribute<TopicAttribute>() is { } topic)
            return topic.Name;

        return Conventional(t);
    });

    public static string NameOf<T>() => NameOf(typeof(T));

    /// <summary>Все имена, под которыми тип принимается на приёме: каноничное + legacy (идея 103).</summary>
    public static IReadOnlyList<string> AliasesOf(Type type) => Aliases.GetOrAdd(type, static t =>
    {
        var names = new List<string> { NameOf(t) };
        if (t.GetCustomAttribute<MessageAliasAttribute>() is { } alias)
            names.AddRange(alias.LegacyNames);
        return names;
    });

    /// <summary>
    /// Конвенция: последний сегмент namespace + kebab-case имя типа.
    /// <c>Contracts.Orders.OrderPlaced</c> → <c>orders.order-placed</c>.
    /// </summary>
    private static string Conventional(Type type)
    {
        var name = type.Name;

        // Обобщённые типы: `1 в имени — не то, что хочется видеть на проводе.
        var tick = name.IndexOf('`');
        if (tick >= 0)
            name = name[..tick];

        var kebab = ToKebabCase(name);
        var ns = type.Namespace;
        if (string.IsNullOrEmpty(ns))
            return kebab;

        var lastDot = ns.LastIndexOf('.');
        var segment = ToKebabCase(lastDot >= 0 ? ns[(lastDot + 1)..] : ns);
        return $"{segment}.{kebab}";
    }

    public static string ToKebabCase(string value)
    {
        if (value.Length == 0)
            return value;

        var builder = new StringBuilder(value.Length + 8);
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsUpper(c))
            {
                // Граница слова: не первый символ и (предыдущий строчный ИЛИ следующий строчный).
                // Второе условие корректно режет аббревиатуры: HTTPServer → http-server.
                var previousIsLower = i > 0 && !char.IsUpper(value[i - 1]);
                var nextIsLower = i + 1 < value.Length && char.IsLower(value[i + 1]);
                if (i > 0 && (previousIsLower || nextIsLower))
                    builder.Append('-');
                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }
}
