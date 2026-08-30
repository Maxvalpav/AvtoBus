# AvtoBus.Generators — Source Generator

> **Code sketch / unverified.** Генератор требует отдельного `netstandard2.0` проекта и snapshot-тестов Roslyn. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Generators/HandlerGenerator.cs

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace AvtoBus.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class HandlerGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 1. Собираем методы Handle/Consume
        var handlers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => IsCandidateMethod(node),
                transform: static (ctx, ct) => ExtractHandler(ctx, ct))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!);

        // 2. Собираем IConsumer<T> классы
        var interfaceConsumers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => IsCandidateClass(node),
                transform: static (ctx, ct) => ExtractInterfaceConsumer(ctx, ct))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!);

        // 3. Валидация: нет дублей, нет orphan commands
        var allHandlers = handlers.Collect().Combine(interfaceConsumers.Collect());
        context.RegisterSourceOutput(allHandlers, static (spc, src) =>
        {
            var methods = src.Left;
            var interfaces = src.Right;
            Validate(spc, methods, interfaces);
        });

        // 4. Эмиттер диспетчеров для method-handlers
        context.RegisterSourceOutput(handlers,
            static (spc, handler) => EmitMethodDispatcher(spc, handler));

        // 5. Эмиттер диспетчеров для interface consumers
        context.RegisterSourceOutput(interfaceConsumers,
            static (spc, consumer) => EmitInterfaceDispatcher(spc, consumer));

        // 6. Эмиттер ModuleInitializer для авторегистрации
        var allMethodTypes = handlers.Collect();
        context.RegisterSourceOutput(allMethodTypes,
            static (spc, list) => EmitModuleInitializer(spc, list));
    }

    private static bool IsCandidateMethod(SyntaxNode node)
    {
        if (node is not MethodDeclarationSyntax method) return false;
        var name = method.Identifier.Text;
        if (name != "Handle" && name != "Consume") return false;
        if (method.ParameterList.Parameters.Count == 0) return false;
        if (method.Modifiers.Any(m => m.IsKind(SyntaxKind.PartialKeyword))) return false;
        return true;
    }

    private static bool IsCandidateClass(SyntaxNode node)
    {
        if (node is not ClassDeclarationSyntax cls) return false;
        return cls.BaseList?.Types.Any(t => t.ToString().Contains("IConsumer")) ?? false;
    }

    private static HandlerInfo? ExtractHandler(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var method = (MethodDeclarationSyntax)ctx.Node;
        var model = ctx.SemanticModel;
        var symbol = model.GetDeclaredSymbol(method, ct) as IMethodSymbol;
        if (symbol is null) return null;

        var firstParam = symbol.Parameters.FirstOrDefault();
        if (firstParam is null) return null;

        var messageType = firstParam.Type;
        if (messageType.Name == "CancellationToken" || messageType.Name == "IServiceProvider")
            return null;

        var messageName = messageType.ToDisplayString();
        var returnType = symbol.ReturnType.ToDisplayString();
        var isAsync = symbol.IsAsync || returnType.Contains("Task");

        var deps = symbol.Parameters.Skip(1)
            .Where(p => p.Type.Name != "CancellationToken")
            .Select(p => new DependencyInfo(p.Type.ToDisplayString(), p.Name, p.IsOptional))
            .ToList();

        return new HandlerInfo
        {
            ContainingType = symbol.ContainingType.ToDisplayString(),
            MethodName = symbol.Name,
            IsStatic = symbol.IsStatic,
            IsAsync = isAsync,
            MessageClrType = messageName,
            ReturnType = returnType,
            Dependencies = deps,
            Location = method.GetLocation(),
            MessageTypeName = ToKebab(messageName.Split('.').Last().Replace("Command", "").Replace("Event", "")),
        };
    }

    private static ConsumerInfo? ExtractInterfaceConsumer(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var cls = (ClassDeclarationSyntax)ctx.Node;
        var model = ctx.SemanticModel;
        var symbol = model.GetDeclaredSymbol(cls, ct) as INamedTypeSymbol;
        if (symbol is null) return null;

        var consumerInterface = symbol.Interfaces
            .FirstOrDefault(i => i.Name.StartsWith("IConsumer"));

        if (consumerInterface is null) return null;

        var messageType = consumerInterface.TypeArguments.FirstOrDefault()?.ToDisplayString();
        if (messageType is null) return null;

        var consumeMethod = symbol.GetMembers()
            .OfType<IMethodSymbol>()
            .FirstOrDefault(m => m.Name == "Consume");

        if (consumeMethod is null) return null;

        return new ConsumerInfo
        {
            ClassName = symbol.ToDisplayString(),
            MessageType = messageType,
            ConsumeMethod = consumeMethod.Name,
            MessageTypeName = ToKebab(messageType.Split('.').Last()),
        };
    }

    private static void EmitMethodDispatcher(SourceProductionContext spc, HandlerInfo handler)
    {
        var className = SanitizeClassName(handler.ContainingType);
        var dispatcherName = $"{className}_{handler.MessageTypeName}_Dispatcher";
        var msgType = handler.MessageClrType;

        var depsBuilder = new StringBuilder();
        for (int i = 0; i < handler.Dependencies.Count; i++)
        {
            var d = handler.Dependencies[i];
            var assignment = d.IsOptional
                ? $"var d{i} = ctx.Services.GetService<{d.TypeName}>();"
                : $"var d{i} = ctx.Services.GetRequiredService<{d.TypeName}>();";
            depsBuilder.AppendLine($"        {assignment}");
        }

        var callArgs = string.Join(", ",
            new[] { "msg" }.Concat(
                handler.Dependencies.Select((_, i) => $"d{i}")));

        var callPrefix = handler.IsAsync ? "await " : "";
        var awaitKeyword = handler.IsAsync ? "await " : "";

        var source = $$"""
            // <auto-generated/>
            #nullable enable
            #pragma warning disable CS8019

            using System;
            using System.Threading;
            using System.Threading.Tasks;
            using Microsoft.Extensions.DependencyInjection;
            using AvtoBus;
            using AvtoBus.Dispatching;

            namespace AvtoBus.Generated;

            [System.CodeDom.Compiler.GeneratedCode("AvtoBus.Generators", "1.0")]
            internal sealed class {{dispatcherName}} : IMessageDispatcher
            {
                public string MessageType => "{{handler.MessageTypeName}}";
                public Type ClrType => typeof({{msgType}});

                public async ValueTask DispatchAsync(ConsumeContext context)
                {
                    var msg = ({{msgType}})context.Message;
            {{depsBuilder}}
            {{caller}}var result = {{handler.IsStatic ? $"{handler.ContainingType}" : $"context.Services.GetRequiredService<{handler.ContainingType}>"}}.{{handler.MethodName}}({{callArgs}});

            {{(handler.ReturnType.Contains("Task")
                ? $"        {awaitKeyword}result;"
                : $"        if (result is not null && result is IEnumerable<object> items) foreach (var item in items) await context.PublishAsync(item);")}}
                }
            }
            """;

        spc.AddSource($"{dispatcherName}.g.cs", source);
    }

    private static void EmitInterfaceDispatcher(SourceProductionContext spc, ConsumerInfo consumer)
    {
        var className = SanitizeClassName(consumer.ClassName);
        var dispatcherName = $"{className}_{consumer.MessageTypeName}_Dispatcher";

        var source = $$"""
            // <auto-generated/>
            #nullable enable
            #pragma warning disable CS8019

            using System;
            using System.Threading;
            using System.Threading.Tasks;
            using Microsoft.Extensions.DependencyInjection;
            using AvtoBus;
            using AvtoBus.Dispatching;

            namespace AvtoBus.Generated;

            [System.CodeDom.Compiler.GeneratedCode("AvtoBus.Generators", "1.0")]
            internal sealed class {{dispatcherName}} : IMessageDispatcher
            {
                public string MessageType => "{{consumer.MessageTypeName}}";
                public Type ClrType => typeof({{consumer.MessageType}});

                public async ValueTask DispatchAsync(ConsumeContext context)
                {
                    var msg = ({{consumer.MessageType}})context.Message;
                    var scope = context.Services.CreateScope();
                    var consumer = scope.ServiceProvider.GetRequiredService<{{consumer.ClassName}}>();

                    try
                    {
                        await consumer.{{consumer.ConsumeMethod}}(msg);
                    }
                    finally
                    {
                        if (scope is IAsyncDisposable asyncDisposable)
                            await asyncDisposable.DisposeAsync();
                        else
                            scope.Dispose();
                    }
                }
            }
            """;

        spc.AddSource($"{dispatcherName}.g.cs", source);
    }

    private static void EmitModuleInitializer(
        SourceProductionContext spc,
        ImmutableArray<HandlerInfo> handlers)
    {
        if (handlers.Length == 0) return;

        var registrations = new StringBuilder();
        foreach (var h in handlers)
        {
            var cls = SanitizeClassName(h.ContainingType);
            var dispatcher = $"{cls}_{h.MessageTypeName}_Dispatcher";
            registrations.AppendLine(
                $"        AvtoBusRegistry.Register(new AvtoBus.Generated.{dispatcher}());");
        }

        var source = $$"""
            // <auto-generated/>
            #nullable enable
            using System.Runtime.CompilerServices;

            namespace AvtoBus.Generated;

            internal static class AvtoBusModuleInit
            {
                [ModuleInitializer]
                internal static void Init()
                {
            {{registrations}}
                }
            }
            """;

        spc.AddSource("AvtoBusModuleInit.g.cs", source);
    }

    private static void Validate(
        SourceProductionContext spc,
        ImmutableArray<HandlerInfo> methods,
        ImmutableArray<ConsumerInfo> interfaces)
    {
        var allHandlers = methods.Cast<HandlerInfo>().Concat(interfaces.Cast<HandlerInfo>()).ToList();
        var byType = allHandlers.GroupBy(h => h.MessageClrType).ToList();

        // AVB002: Multiple handlers for same command
        foreach (var group in byType.Where(g => g.Count() > 1))
        {
            var loc = group.First().Location;
            spc.ReportDiagnostic(Diagnostic.Create(
                new DiagnosticDescriptor(
                    "AVB002",
                    "Multiple handlers for command",
                    "Message '{0}' has {1} handlers; commands must have exactly one",
                    "AvtoBus",
                    DiagnosticSeverity.Error,
                    isEnabledByDefault: true),
                loc, group.Key, group.Count()));
        }
    }

    private static string ToKebab(string name)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i]))
                sb.Append('-');
            sb.Append(char.ToLowerInvariant(name[i]));
        }
        return sb.ToString();
    }

    private static string SanitizeClassName(string fullName)
        => fullName.Replace(".", "_").Replace("+", "_").Replace("<", "_").Replace(">", "_")
                   .Replace(",", "_").Replace(" ", "");
}

internal sealed class HandlerInfo
{
    public string ContainingType { get; set; } = "";
    public string MethodName { get; set; } = "";
    public bool IsStatic { get; set; }
    public bool IsAsync { get; set; }
    public string MessageClrType { get; set; } = "";
    public string MessageTypeName { get; set; } = "";
    public string ReturnType { get; set; } = "";
    public List<DependencyInfo> Dependencies { get; set; } = new();
    public Location Location { get; set; }
}

internal sealed class ConsumerInfo
{
    public string ClassName { get; set; } = "";
    public string MessageType { get; set; } = "";
    public string ConsumeMethod { get; set; } = "";
    public string MessageTypeName { get; set; } = "";
}

internal sealed record DependencyInfo(string TypeName, string ParamName, bool IsOptional);
```
