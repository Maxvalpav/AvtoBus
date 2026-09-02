namespace AvtoBus;

/// <summary>Разрешает тип контракта по wire-имени. Allowlist-режим — только зарегистрированные типы.</summary>
public interface ITypeResolver
{
    bool TryResolve(string messageType, out Type type);
    string NameOf(Type type);
}
