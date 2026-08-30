using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace AvtoBus;

/// <summary>
/// Таблица «строковое имя контракта ↔ CLR-тип». Строится при старте и замораживается:
/// lookup по <see cref="FrozenDictionary{TKey,TValue}"/> быстрее обычного словаря (идея 363).
/// </summary>
public sealed class MessageRegistry
{
    private readonly FrozenDictionary<string, Type> _byName;
    private readonly FrozenDictionary<Type, string> _byType;

    private MessageRegistry(FrozenDictionary<string, Type> byName, FrozenDictionary<Type, string> byType)
    {
        _byName = byName;
        _byType = byType;
    }

    public IEnumerable<Type> Types => _byType.Keys;

    public static MessageRegistry Build(IEnumerable<Type> types)
    {
        var byName = new Dictionary<string, Type>(StringComparer.Ordinal);
        var byType = new Dictionary<Type, string>();

        foreach (var type in types)
        {
            var canonical = MessageTypeNaming.NameOf(type);
            byType[type] = canonical;

            // Legacy-алиасы разрешаются в тот же тип, но каноничным остаётся только один.
            foreach (var alias in MessageTypeNaming.AliasesOf(type))
            {
                if (byName.TryGetValue(alias, out var existing) && existing != type)
                    throw new InvalidOperationException(
                        $"Имя контракта '{alias}' занято типами {existing.FullName} и {type.FullName}. " +
                        "Задайте [MessageAlias] хотя бы одному из них.");

                byName[alias] = type;
            }
        }

        return new MessageRegistry(
            byName.ToFrozenDictionary(StringComparer.Ordinal),
            byType.ToFrozenDictionary());
    }

    public bool TryResolve(string messageType, [NotNullWhen(true)] out Type? type)
        => _byName.TryGetValue(messageType, out type);

    public string NameOf(Type type)
        => _byType.TryGetValue(type, out var name) ? name : MessageTypeNaming.NameOf(type);

    public string NameOf<T>() => NameOf(typeof(T));
}
