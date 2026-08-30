using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace AvtoBus.Generators;

/// <summary>
/// Данные о методе-хендлере, извлечённые из компиляции.
/// </summary>
internal sealed record HandlerInfo(
    string ContainingType,
    string MethodName,
    bool IsStatic,
    bool IsAsync,
    string MessageClrType,
    string MessageType,
    ReturnKind ReturnKind,
    bool IsCommand,
    ImmutableArray<DependencyInfo> Dependencies,
    Location Location,
    string HandlerName);

/// <summary>Какого рода результат возвращает хендлер — определяет код эмиттера.</summary>
internal enum ReturnKind
{
    /// <summary>void — вызов без каскада.</summary>
    Void,

    /// <summary>Task / ValueTask — await без каскада.</summary>
    AwaitVoid,

    /// <summary>Task&lt;T&gt; / ValueTask&lt;T&gt; — await и каскад результата.</summary>
    AwaitValue,

    /// <summary>Синхронный результат — сразу каскад.</summary>
    SyncValue,
}

/// <summary>Параметр метода, инжектируемый из scoped-контейнера сообщения.</summary>
internal sealed record DependencyInfo(string TypeName, string ParamName);

/// <summary>
/// Типы-диспетчеры, зарегистрированные через <c>IMessageDispatcher</c>.
/// </summary>
internal sealed record InterfaceConsumerInfo(
    string HandlerType,
    string MessageClrType,
    string MessageType,
    bool IsCommand,
    Location Location,
    string HandlerName);

internal static class HandlerModel
{
    public const string BatchNamespace = "AvtoBus.Generated";
}
