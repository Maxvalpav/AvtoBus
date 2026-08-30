using System.Collections.Immutable;
using System.Composition;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace AvtoBus.Analyzers;

/// <summary>Code fix для AVB004/AVB005: Publish ↔ Send.</summary>
[ExportCodeFixProvider(LanguageNames.CSharp), Shared]
public sealed class PublishCommandCodeFix : CodeFixProvider
{
    public override ImmutableArray<string> FixableDiagnosticIds
        => ImmutableArray.Create(Rules.AVB004.Id, Rules.AVB005.Id);

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
        if (root is null)
            return;

        foreach (var diagnostic in context.Diagnostics)
        {
            var node = root.FindNode(diagnostic.Location.SourceSpan);
            if (node is not InvocationExpressionSyntax invocation)
                continue;

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                continue;

            var currentMethod = memberAccess.Name.Identifier.Text;
            var newMethod = currentMethod switch
            {
                "Publish" => "Send",
                "PublishAsync" => "SendAsync",
                "Send" => "Publish",
                "SendAsync" => "PublishAsync",
                _ => currentMethod == "Publish" ? "Send" : "Publish",
            };
            var title = $"Replace '{currentMethod}' with '{newMethod}'";

            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    _ =>
                    {
                        var newName = memberAccess.WithName(SyntaxFactory.IdentifierName(newMethod).WithTriviaFrom(memberAccess.Name));
                        var newInvocation = invocation.WithExpression(newName).WithTriviaFrom(invocation);
                        var newRoot = root.ReplaceNode(invocation, newInvocation);
                        return Task.FromResult(context.Document.WithSyntaxRoot(newRoot));
                    },
                    equivalenceKey: title),
                diagnostic);
        }
    }
}
