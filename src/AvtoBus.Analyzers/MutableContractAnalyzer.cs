using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AvtoBus.Analyzers;

/// <summary>
/// AVB010/017/022: контракты сообщений immutable, без TenantId в теле, без «god-событий».
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MutableContractAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics
        => ImmutableArray.Create(Rules.AVB010, Rules.AVB017, Rules.AVB022);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeType, SymbolKind.NamedType);
    }

    private static void AnalyzeType(SymbolAnalysisContext context)
    {
        if (context.Symbol is not INamedTypeSymbol type)
            return;

        var isMessage = type.AllInterfaces.Any(i => i.ToDisplayString() is "AvtoBus.ICommand" or "AvtoBus.IEvent" or "AvtoBus.IRequest");
        if (!isMessage)
            return;

        var properties = type.GetMembers().OfType<IPropertySymbol>().ToArray();

        // AVB010: сеттеры вместо init ломают иммутабельность контракта.
        foreach (var prop in properties)
        {
            if (prop.SetMethod is not null && !prop.SetMethod.IsInitOnly)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rules.AVB010, prop.Locations.FirstOrDefault() ?? type.Locations.First(), type.Name));
            }
        }

        // AVB017: TenantId должен жить в конверте (Envelope.TenantId), а не в теле.
        if (properties.Any(p => p.Name.Equals("TenantId", System.StringComparison.OrdinalIgnoreCase)))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB017, type.Locations.FirstOrDefault(), type.Name));
        }

        // AVB022: god-событие — больше 20 полей и суффикс Updated.
        if (properties.Length > 20 && type.Name.Contains("Updated"))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB022, type.Locations.FirstOrDefault(), type.Name, properties.Length));
        }
    }
}
