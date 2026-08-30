using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace AvtoBus.Generators;

/// <summary>
/// Диагностики: ошибки на этапе сборки вместо NoHandlerException в проде (док 16, §5).
/// </summary>
internal static class Diagnostics
{
    internal static readonly DiagnosticDescriptor AVB001 = new(
        id: "AVB001",
        title: "No handler registered for command",
        messageFormat: "Command '{0}' is sent but no handler is registered",
        category: "AvtoBus",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor AVB002 = new(
        id: "AVB002",
        title: "Multiple handlers for command",
        messageFormat: "Command '{0}' has {1} handlers; commands must have exactly one",
        category: "AvtoBus",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor AVB003 = new(
        id: "AVB003",
        title: "Event has no subscribers",
        messageFormat: "Event '{0}' is published but no consumer subscribes to it",
        category: "AvtoBus",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor AVB010 = new(
        id: "AVB010",
        title: "Mutable contract",
        messageFormat: "Message contract '{0}' has mutable properties; use init-only",
        category: "AvtoBus",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    /// <summary>
    /// AVB008: первый параметр хендлера не является message contract. Номер не совпадает с
    /// таблицей ADR-0004 из-за коллизии: AVB004/AVB005 в анализаторе заняты Publish/Send misuse
    /// (PublishCommandAnalyzer). Согласован с <c>BusConfigurator.IsPlausibleMessageType</c>.
    /// </summary>
    internal static readonly DiagnosticDescriptor AVB008 = new(
        id: "AVB008",
        title: "First parameter is not a message contract",
        messageFormat: "First parameter '{0}' of a handler is not a message contract; use a concrete command/event type",
        category: "AvtoBus",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static void Validate(
        SourceProductionContext spc,
        ImmutableArray<HandlerInfo> methods,
        ImmutableArray<InterfaceConsumerInfo> interfaces,
        Compilation compilation)
    {
        // AVB002: несколько хендлеров на одну команду — команда обязана иметь ровно одного владельца.
        var commandGroups = ToHandler(methods, interfaces)
            .Where(static h => h.IsCommand)
            .GroupBy(static h => h.MessageClrType)
            .Where(static g => g.Count() > 1);

        foreach (var group in commandGroups)
        {
            foreach (var h in group)
            {
                spc.ReportDiagnostic(Diagnostic.Create(AVB002, h.Location, group.Key, group.Count()));
            }
        }

        // AVB010: контракт с мутабельными свойствами — сериализуется из других процессов.
        var handledContracts = methods.Select(static h => h.MessageClrType)
            .Concat(interfaces.Select(static c => c.MessageClrType))
            .Distinct();

        foreach (var contract in handledContracts)
        {
            var metadataName = contract.StartsWith("global::", StringComparison.Ordinal)
                ? contract.Substring("global::".Length)
                : contract;

            var symbol = compilation.GetTypeByMetadataName(metadataName);
            if (symbol is null)
                continue;

            foreach (var property in symbol.GetMembers().OfType<IPropertySymbol>())
            {
                if (property.SetMethod is { IsInitOnly: false } setter
                    && setter.DeclaredAccessibility != Accessibility.Private)
                {
                    var location = property.Locations.FirstOrDefault() ?? symbol.Locations.First();
                    spc.ReportDiagnostic(Diagnostic.Create(AVB010, location, contract));
                    break;
                }
            }
        }
    }

    /// <summary>Метод-хендлеры в общей форме для диагностик.</summary>
    private static IEnumerable<HandlerInfo> ToHandler(
        ImmutableArray<HandlerInfo> methods,
        ImmutableArray<InterfaceConsumerInfo> interfaces)
    {
        foreach (var m in methods)
            yield return m;

        foreach (var c in interfaces)
        {
            yield return new HandlerInfo(
                c.HandlerType,
                "Consume",
                IsStatic: false,
                IsAsync: true,
                c.MessageClrType,
                c.MessageType,
                ReturnKind.AwaitVoid,
                c.IsCommand,
                ImmutableArray<DependencyInfo>.Empty,
                c.Location,
                c.HandlerName);
        }
    }

    /// <summary>AVB008: первый параметр хендлера — заведомо не контракт.</summary>
    public static void ValidateBadHandlers(
        SourceProductionContext spc,
        ImmutableArray<BadHandlerInfo> badHandlers)
    {
        foreach (var bad in badHandlers)
            spc.ReportDiagnostic(Diagnostic.Create(AVB008, bad.Location, bad.TypeName));
    }

    /// <summary>Для публикации/отправки через IBus — предупреждение о событиях без подписчиков.</summary>
    public static void ValidateInvocations(
        SourceProductionContext spc,
        ImmutableArray<InvocationInfo> invocations,
        ImmutableArray<HandlerInfo> methods,
        ImmutableArray<InterfaceConsumerInfo> interfaces)
    {
        if (invocations.Length == 0)
            return;

        var handled = new HashSet<string>(
            methods.Select(static h => h.MessageClrType)
                .Concat(interfaces.Select(static c => c.MessageClrType)));

        foreach (var invocation in invocations)
        {
            if (invocation.IsEvent)
            {
                if (!handled.Contains(invocation.MessageClrType))
                {
                    spc.ReportDiagnostic(Diagnostic.Create(AVB003, invocation.Location, invocation.MessageClrType));
                }
            }
            else if (!handled.Contains(invocation.MessageClrType))
            {
                // AVB001: команда отправлена, но ни один хендлер её не обрабатывает —
                // молчаливый NoHandlerException в проде вместо ошибки на этапе сборки.
                spc.ReportDiagnostic(Diagnostic.Create(AVB001, invocation.Location, invocation.MessageClrType));
            }
        }
    }
}

/// <summary>Данные о вызове Send/Publish в пользовательском коде.</summary>
internal sealed record InvocationInfo(string MessageClrType, bool IsEvent, Location Location);

/// <summary>Метод-хендлер по конвенции с первым параметром, заведомо не являющимся контрактом.</summary>
internal sealed record BadHandlerInfo(string TypeName, Location Location);
