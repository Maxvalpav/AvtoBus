# AvtoBus.Analyzers — Roslyn Analyzers + Code Fixes

---

## AvtoBus.Analyzers/Rules.cs

```csharp
using Microsoft.CodeAnalysis;

namespace AvtoBus.Analyzers;

public static class Rules
{
    // ── Commands & Events ──

    public static readonly DiagnosticDescriptor AVB001 = new(
        "AVB001", "No handler for command",
        "Command '{0}' is sent via bus.Send() but no handler (Handle/Consume) is registered for it",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB001");

    public static readonly DiagnosticDescriptor AVB002 = new(
        "AVB002", "Multiple handlers for command",
        "Command '{0}' has {1} handlers — commands must have exactly one handler",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB002");

    public static readonly DiagnosticDescriptor AVB003 = new(
        "AVB003", "Event has no subscribers",
        "Event '{0}' is published but no consumer subscribes — consider adding a handler or removing the publish",
        "AvtoBus.Routing", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB003");

    public static readonly DiagnosticDescriptor AVB004 = new(
        "AVB004", "Publish used for ICommand",
        "'{0}' implements ICommand but is sent via Publish() — use Send() for commands",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB004");

    public static readonly DiagnosticDescriptor AVB005 = new(
        "AVB005", "Send used for IEvent",
        "'{0}' implements IEvent but is sent via Send() — use Publish() for events",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB005");

    // ── Contracts ──

    public static readonly DiagnosticDescriptor AVB010 = new(
        "AVB010", "Mutable contract",
        "Message contract '{0}' has settable properties — use 'init' or 'required init' for immutability",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB010");

    public static readonly DiagnosticDescriptor AVB011 = new(
        "AVB011", "Contract references domain type",
        "Contract '{0}' references domain type '{1}' — contracts must be standalone DTOs",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB011");

    public static readonly DiagnosticDescriptor AVB015 = new(
        "AVB015", "Decimal without currency",
        "Property '{0}.{1}' is decimal without [Currency] — financial amounts need explicit currency",
        "AvtoBus.Contracts", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB015");

    public static readonly DiagnosticDescriptor AVB017 = new(
        "AVB017", "TenantId in contract body",
        "Property 'TenantId' found in contract '{0}' — tenant should be in Envelope.TenantId, not body",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB017");

    public static readonly DiagnosticDescriptor AVB020 = new(
        "AVB020", "Large contract",
        "Contract '{0}' serializes to ~{1} KB — consider using Claim Check pattern for payloads > 64KB",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB020");

    public static readonly DiagnosticDescriptor AVB022 = new(
        "AVB022", "God event",
        "Event '{0}' has {1} properties and name '*Updated' — consider splitting into specific domain events",
        "AvtoBus.Contracts", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB022");

    // ── Sagas ──

    public static readonly DiagnosticDescriptor AVB040 = new(
        "AVB040", "Saga queries external service",
        "Saga '{0}' calls external service '{1}' — carry data in events instead",
        "AvtoBus.Sagas", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB040");

    public static readonly DiagnosticDescriptor AVB041 = new(
        "AVB041", "Saga without timeout",
        "Saga '{0}' sends command '{1}' but has no timeout — add RequestTimeout to prevent stuck sagas",
        "AvtoBus.Sagas", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB041");

    // ── Event Sourcing ──

    public static readonly DiagnosticDescriptor AVB050 = new(
        "AVB050", "Cross-aggregate read",
        "Aggregate '{0}' loads another aggregate '{1}' — read from a projection or listen for events",
        "AvtoBus.EventSourcing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB050");

    // ── Naming ──

    public static readonly DiagnosticDescriptor AVB060 = new(
        "AVB060", "Event not past tense",
        "Event '{0}' should be named in past tense (e.g., 'OrderPlaced' not 'PlaceOrderEvent')",
        "AvtoBus.Naming", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB060");
}
```

---

## AvtoBus.Analyzers/PublishCommandAnalyzer.cs (AVB004/AVB005)

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Immutable;

namespace AvtoBus.Analyzers;

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

    private void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        var memberAccess = invocation.Expression as MemberAccessExpressionSyntax;
        if (memberAccess is null) return;

        var methodName = memberAccess.Name.Identifier.Text;
        if (methodName is not ("Publish" or "Send")) return;

        var symbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
        if (symbol?.ContainingType.ToDisplayString() != "AvtoBus.IBus") return;

        var typeArg = symbol.TypeArguments.FirstOrDefault();
        if (typeArg is null) return;

        var isCommand = typeArg.AllInterfaces.Any(i => i.Name == "ICommand");
        var isEvent = typeArg.AllInterfaces.Any(i => i.Name == "IEvent");

        // AVB004: Publish(ICommand) → должен быть Send
        if (methodName == "Publish" && isCommand)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB004, invocation.GetLocation(), typeArg.Name));
        }

        // AVB005: Send(IEvent) → должен быть Publish
        if (methodName == "Send" && isEvent)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB005, invocation.GetLocation(), typeArg.Name));
        }
    }
}
```

---

## AvtoBus.Analyzers/MutableContractAnalyzer.cs (AVB010)

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Immutable;

namespace AvtoBus.Analyzers;

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

    private void AnalyzeType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        var isMessage = type.AllInterfaces.Any(i => i.Name is "ICommand" or "IEvent" or "IRequest");
        if (!isMessage) return;

        // AVB010: Mutable properties
        foreach (var prop in type.GetMembers().OfType<IPropertySymbol>())
        {
            if (prop.SetMethod is not null && !prop.SetMethod.IsInitOnly)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rules.AVB010, prop.Locations.First(), type.Name));
            }
        }

        // AVB017: TenantId in contract body
        var hasTenantProp = type.GetMembers()
            .OfType<IPropertySymbol>()
            .Any(p => p.Name.Equals("TenantId", StringComparison.OrdinalIgnoreCase));
        if (hasTenantProp)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB017, type.Locations.First(), type.Name));
        }

        // AVB022: God event (>20 properties + name contains "Updated")
        var propCount = type.GetMembers().OfType<IPropertySymbol>().Count();
        if (propCount > 20 && type.Name.Contains("Updated"))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB022, type.Locations.First(), type.Name, propCount));
        }
    }
}
```

---

## AvtoBus.Analyzers/PublishCommandCodeFix.cs (code fix для AVB004)

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Immutable;
using System.Composition;

namespace AvtoBus.Analyzers;

[ExportCodeFixProvider(LanguageNames.CSharp), Shared]
public sealed class PublishCommandCodeFix : CodeFixProvider
{
    public override ImmutableArray<string> FixableDiagnosticIds
        => ImmutableArray.Create(Rules.AVB004.Id, Rules.AVB005.Id);

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken);
        if (root is null) return;

        foreach (var diagnostic in context.Diagnostics)
        {
            var node = root.FindNode(diagnostic.Location.SourceSpan);
            if (node is not InvocationExpressionSyntax invocation) continue;
            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess) continue;

            var currentMethod = memberAccess.Name.Identifier.Text;
            var newMethod = currentMethod == "Publish" ? "Send" : "Publish";
            var title = $"Replace '{currentMethod}' with '{newMethod}'";

            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    ct =>
                    {
                        var newName = memberAccess.WithName(
                            SyntaxFactory.IdentifierName(newMethod));
                        var newInvocation = invocation.WithExpression(newName);
                        var newRoot = root.ReplaceNode(invocation, newInvocation);
                        return Task.FromResult(context.Document.WithSyntaxRoot(newRoot));
                    },
                    equivalenceKey: title),
                diagnostic);
        }
    }
}
```

---

## AvtoBus.Analyzers/NamingAnalyzer.cs (AVB060)

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Immutable;

namespace AvtoBus.Analyzers;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NamingAnalyzer : DiagnosticAnalyzer
{
    private static readonly string[] _pastSuffixes =
        { "ed", "en", "ied", "ted", "sed", "ged", "ned" };

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics
        => ImmutableArray.Create(Rules.AVB060);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeType, SymbolKind.NamedType);
    }

    private void AnalyzeType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        var isEvent = type.AllInterfaces.Any(i => i.Name == "IEvent");
        if (!isEvent) return;

        var name = type.Name;

        // Пропускаем если уже в прошедшем времени
        if (_pastSuffixes.Any(s => name.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
            return;

        // Пропускаем если содержит "Event" в конце (legacy)
        if (name.EndsWith("Event"))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB060, type.Locations.First(), name));
            return;
        }

        // Эвристика: имя начинается с глагола (Create, Update, Delete, Place, ...)
        var verbs = new[] { "Create", "Update", "Delete", "Place", "Send", "Start", "Process", "Handle", "Add", "Remove" };
        if (verbs.Any(v => name.StartsWith(v)))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rules.AVB060, type.Locations.First(), name));
        }
    }
}
```

---

## AvtoBus.Analyzers/AvtoBus.Analyzers.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>disable</ImplicitUsings>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <EnforceExtendedAnalyzerRules>true</EnforceExtendedAnalyzerRules>
    <Description>Roslyn analyzers and code fixes for AvtoBus contracts, routing and naming.</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.Workspaces" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" PrivateAssets="all" />
  </ItemGroup>

  <ItemGroup>
    <None Include="$(OutputPath)\$(AssemblyName).dll"
          Pack="true"
          PackagePath="analyzers/dotnet/cs"
          Visible="false" />
  </ItemGroup>

</Project>
```
