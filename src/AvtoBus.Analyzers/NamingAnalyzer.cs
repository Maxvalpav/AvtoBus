using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AvtoBus.Analyzers;

/// <summary>AVB060: события именуются в прошедшем времени (OrderPlaced, а не PlaceOrder).</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NamingAnalyzer : DiagnosticAnalyzer
{
    private static readonly string[] PastSuffixes =
        { "ed", "en", "ied", "ted", "sed", "ged", "ned", "ted", "ded" };

    private static readonly string[] VerbPrefixes =
        { "Create", "Update", "Delete", "Place", "Send", "Start", "Process", "Handle", "Add", "Remove" };

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics
        => ImmutableArray.Create(Rules.AVB060);

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

        var isEvent = type.AllInterfaces.Any(i => i.ToDisplayString() == "AvtoBus.IEvent");
        if (!isEvent)
            return;

        var name = type.Name;

        // Уже в прошедшем времени (OrderPlaced, MoneyDeposited) — ок.
        if (PastSuffixes.Any(s => name.EndsWith(s, System.StringComparison.OrdinalIgnoreCase)))
            return;

        // Legacy: *Event в конце — флаг, но это переходный период.
        if (name.EndsWith("Event"))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB060, type.Locations.FirstOrDefault(), name));
            return;
        }

        // Имя начинается с глагола в настоящем (PlaceOrder, SendInvoice) — будущее событие.
        if (VerbPrefixes.Any(v => name.StartsWith(v, System.StringComparison.Ordinal)))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB060, type.Locations.FirstOrDefault(), name));
        }
    }
}
