using System.CommandLine;
using System.Reflection;

namespace AvtoBus.Cli;

/// <summary>
/// Сканирует сборку на контракты (ICommand/IEvent) через рефлексию — без DI и шины.
/// Используется командами contracts и es explain.
/// </summary>
internal static class ContractScanner
{
    public static IReadOnlyList<Type> Scan(Assembly assembly)
    {
        var result = new List<Type>();
        foreach (var type in assembly.GetTypes())
        {
            if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                continue;

            if (typeof(ICommand).IsAssignableFrom(type) || typeof(IEvent).IsAssignableFrom(type))
                result.Add(type);
        }
        return result.OrderBy(t => t.FullName, StringComparer.Ordinal).ToArray();
    }

    public static Type? Resolve(IReadOnlyList<Type> types, string name)
    {
        var lowered = name.ToLowerInvariant();
        return types.FirstOrDefault(t =>
            t.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
            t.Name.ToLowerInvariant() == lowered ||
            (t.FullName ?? "").Equals(name, StringComparison.OrdinalIgnoreCase));
    }
}
