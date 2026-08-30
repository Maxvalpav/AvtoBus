using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AvtoBus.Analyzers;

/// <summary>
/// AVB008: первый параметр хендлера по конвенции (Handle/Consume/HandleAsync/ConsumeAsync)
/// обязан быть message contract. Дублирует проверку генератора (Diagnostics.ValidateBadHandlers)
/// для IDE-опыта и согласован с runtime-фильтром BusConfigurator.IsPlausibleMessageType:
/// string/примитивы/object/decimal/DateTime/Guid/enum/массив — не контракты.
/// Интерфейсный параметр — намеренный отказ (полиморфизм), не ошибка.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class HandlerContractAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics
        => ImmutableArray.Create(Rules.AVB008);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.MethodDeclaration);
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
    {
        var methodSyntax = (MethodDeclarationSyntax)context.Node;
        if (methodSyntax.Identifier.ValueText is not ("Handle" or "Consume" or "HandleAsync" or "ConsumeAsync"))
            return;

        if (context.SemanticModel.GetDeclaredSymbol(methodSyntax, context.CancellationToken) is not IMethodSymbol method)
            return;

        if (method.MethodKind != MethodKind.Ordinary)
            return;

        if (method.Parameters.Length == 0)
            return;

        var first = method.Parameters[0];

        // Интерфейсный/типопараметрический первый параметр — намеренный отказ
        // (полиморфизм нельзя разрешить на этапе компиляции), а не ошибка контракта.
        if (first.Type.TypeKind is TypeKind.Interface or TypeKind.TypeParameter)
            return;

        if (!IsNonContract(first.Type))
            return;

        var location = first.Type.Locations.FirstOrDefault() ?? methodSyntax.GetLocation();
        context.ReportDiagnostic(Diagnostic.Create(
            Rules.AVB008, location, first.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)));
    }

    private static bool IsNonContract(ITypeSymbol type)
    {
        if (type.SpecialType != SpecialType.None)
            return true; // string, object, decimal, DateTime, все примитивы

        if (type.TypeKind is TypeKind.Enum or TypeKind.Array or TypeKind.Pointer)
            return true;

        return type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) == "global::System.Guid";
    }
}
