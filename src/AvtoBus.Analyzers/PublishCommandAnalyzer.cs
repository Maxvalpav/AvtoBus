using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AvtoBus.Analyzers;

/// <summary>AVB004/AVB005: команды через Send, события через Publish.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PublishCommandAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics
        => ImmutableArray.Create(Rules.AVB004, Rules.AVB005);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return;

        var methodName = memberAccess.Name.Identifier.Text;
        if (methodName is not ("Publish" or "PublishAsync" or "Send" or "SendAsync"))
            return;

        var symbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
        if (symbol?.ContainingType?.ToDisplayString() != "AvtoBus.IBus")
            return;

        var typeArg = symbol.TypeArguments.FirstOrDefault();
        if (typeArg is null)
            return;

        var isCommand = Implements(typeArg, "AvtoBus.ICommand");
        var isEvent = Implements(typeArg, "AvtoBus.IEvent");

        var isPublish = methodName is "Publish" or "PublishAsync";
        var isSend = methodName is "Send" or "SendAsync";

        if (isPublish && isCommand)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB004, invocation.GetLocation(), typeArg.Name));
        }

        if (isSend && isEvent)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB005, invocation.GetLocation(), typeArg.Name));
        }
    }

    private static bool Implements(ITypeSymbol type, string fullName)
    {
        var current = type;
        while (current is not null)
        {
            if (current.ToDisplayString() == fullName)
                return true;

            foreach (var iface in current.AllInterfaces)
            {
                if (iface.ToDisplayString() == fullName)
                    return true;
            }

            current = current.BaseType;
        }

        return false;
    }
}
