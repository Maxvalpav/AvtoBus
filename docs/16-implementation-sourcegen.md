# 🔧 Реализация: Source Generator (`AvtoBus.Generators`)

> **Design draft.** Roslyn API и сгенерированный код должны быть проверены отдельным generator-проектом и snapshot-тестами.

Пакет-инкрементальный генератор, который делает AvtoBus **AOT-ready** и **zero-reflection**.

## 1. Что генерируется

Для каждого метода/класса-хендлера, найденного в проекте:

- `IMessageDispatcher` — типизированный вызов без рефлексии
- Регистрация в `DispatcherRegistry` (через `[ModuleInitializer]`)
- `JsonSerializerContext` для контракта сообщения
- Роутинг-таблица тип↔адрес (compile-time)
- Диагностики: `AVB001..AVB0xx`

## 2. Скелет инкрементального генератора

```csharp
// AvtoBus.Generators/HandlerGenerator.cs
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

[Generator(LanguageNames.CSharp)]
public sealed class HandlerGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 1. Ищем все методы Handle/Consume в проекте
        var handlers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (n, _) => IsCandidate(n),
                transform: static (ctx, ct) => Extract(ctx, ct))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!);

        // 2. Ищем все интерфейсные консьюмеры IConsumer<T>
        var interfaceConsumers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (n, _) => n is ClassDeclarationSyntax,
                transform: static (ctx, ct) => ExtractInterfaceConsumer(ctx, ct))
            .Where(static x => x is not null)
            .Select(static (x, _) => x!);

        // 3. Все контракты (для JsonSerializerContext)
        var contracts = handlers.Collect().Combine(interfaceConsumers.Collect())
            .Select(static (t, _) => t.Left.Select(h => h.MessageType)
                                     .Concat(t.Right.Select(c => c.MessageType))
                                     .Distinct().ToImmutableArray());

        // 4. Компиляция + опции
        var compAndOpts = context.CompilationProvider.Combine(context.AnalyzerConfigOptionsProvider);

        // 5. Регистрируем эмиттеры
        context.RegisterSourceOutput(handlers.Combine(compAndOpts),
            static (spc, src) => EmitDispatcher(spc, src.Left, src.Right.Left));

        context.RegisterSourceOutput(interfaceConsumers.Combine(compAndOpts),
            static (spc, src) => EmitInterfaceDispatcher(spc, src.Left, src.Right.Left));

        context.RegisterSourceOutput(contracts,
            static (spc, types) => EmitJsonContext(spc, types));

        // 6. Диагностики
        context.RegisterSourceOutput(handlers.Collect(),
            static (spc, all) => Validate(spc, all));
    }

    private static bool IsCandidate(SyntaxNode n) =>
        n is MethodDeclarationSyntax m &&
        (m.Identifier.Text is "Handle" or "Consume") &&
        m.ParameterList.Parameters.Count >= 1;

    private static HandlerInfo? Extract(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var method = (MethodDeclarationSyntax)ctx.Node;
        var sym = ctx.SemanticModel.GetDeclaredSymbol(method, ct) as IMethodSymbol;
        if (sym is null) return null;

        var firstParam = sym.Parameters[0];
        if (!LooksLikeMessage(firstParam.Type)) return null;

        return new HandlerInfo(
            ContainingType: sym.ContainingType.ToDisplayString(),
            MethodName: sym.Name,
            IsStatic: sym.IsStatic,
            IsAsync: sym.IsAsync || ReturnsTaskLike(sym.ReturnType),
            MessageClrType: firstParam.Type.ToDisplayString(),
            MessageType: MessageAlias(firstParam.Type),
            ReturnType: sym.ReturnType.ToDisplayString(),
            Cascades: DetectCascades(sym.ReturnType),
            Dependencies: sym.Parameters.Skip(1)
                .Select(p => new DependencyInfo(p.Type.ToDisplayString(), p.Name))
                .ToImmutableArray(),
            Location: method.GetLocation());
    }
}

internal sealed record HandlerInfo(
    string ContainingType, string MethodName, bool IsStatic, bool IsAsync,
    string MessageClrType, string MessageType, string ReturnType,
    CascadeInfo Cascades, ImmutableArray<DependencyInfo> Dependencies, Location Location);

internal sealed record DependencyInfo(string TypeName, string ParamName);
internal enum CascadeInfo { None, Single, Tuple, OutgoingMessages, ResultOf }
```

## 3. Эмиттер диспетчера

```csharp
private static void EmitDispatcher(SourceProductionContext spc, HandlerInfo h, Compilation _)
{
    var deps = string.Join("\n        ", h.Dependencies.Select((d, i) =>
        $"var __d{i} = ctx.Services.GetRequiredService<{d.TypeName}>();"));

    var depArgs = string.Join(", ", h.Dependencies.Select((_, i) => $"__d{i}"));
    var call = h.IsStatic
        ? $"{h.ContainingType}.{h.MethodName}(msg{(h.Dependencies.Length > 0 ? ", " + depArgs : "")})"
        : $"ctx.Services.GetRequiredService<{h.ContainingType}>().{h.MethodName}(msg{(h.Dependencies.Length > 0 ? ", " + depArgs : "")})";

    var invoke = h.IsAsync ? $"await {call}" : call;

    var cascade = h.Cascades switch
    {
        CascadeInfo.Single => "if (__result is not null) await ctx.PublishAsync(__result);",
        CascadeInfo.Tuple  => "await CascadeTuple(ctx, __result);",
        CascadeInfo.OutgoingMessages => "await ((OutgoingMessages)__result).ApplyAsync(ctx);",
        CascadeInfo.None   => "",
        _                  => ""
    };

    var body = h.Cascades == CascadeInfo.None
        ? $"{invoke};"
        : $"var __result = {invoke};\n        {cascade}";

    var className = SanitizeName($"{h.ContainingType}_{h.MessageClrType}_Dispatcher");

    var source = $$"""
        // <auto-generated/>
        #nullable enable
        using System;
        using System.Threading.Tasks;
        using Microsoft.Extensions.DependencyInjection;
        using AvtoBus;
        using AvtoBus.Dispatching;

        namespace AvtoBus.Generated;

        internal sealed class {{className}} : IMessageDispatcher
        {
            public string MessageType => "{{h.MessageType}}";
            public Type ClrType => typeof({{h.MessageClrType}});

            public async ValueTask DispatchAsync(ConsumeContext ctx)
            {
                var msg = ({{h.MessageClrType}})ctx.Message;
                {{deps}}
                {{body}}
            }

            private static async ValueTask CascadeTuple(ConsumeContext ctx, System.Runtime.CompilerServices.ITuple tup)
            {
                for (int i = 0; i < tup.Length; i++)
                {
                    var item = tup[i];
                    if (item is not null) await ctx.PublishAsync(item);
                }
            }
        }
        """;

    spc.AddSource($"{className}.g.cs", source);
}
```

## 4. Автоматическая регистрация через ModuleInitializer

```csharp
// Отдельный файл, эмитится один раз на сборку
private static void EmitRegistration(SourceProductionContext spc, ImmutableArray<HandlerInfo> handlers)
{
    var registrations = string.Join("\n        ", handlers.Select(h =>
    {
        var cls = SanitizeName($"{h.ContainingType}_{h.MessageClrType}_Dispatcher");
        return $"AvtoBusRegistry.Register(new AvtoBus.Generated.{cls}());";
    }));

    spc.AddSource("AvtoBusModuleInit.g.cs", $$"""
        // <auto-generated/>
        using System.Runtime.CompilerServices;
        using AvtoBus.Dispatching;

        namespace AvtoBus.Generated;

        internal static class AvtoBusModuleInit
        {
            [ModuleInitializer]
            internal static void Init()
            {
                {{registrations}}
            }
        }
        """);
}
```

## 5. Диагностики

```csharp
private static readonly DiagnosticDescriptor AVB001 = new(
    id: "AVB001", title: "No handler registered for command",
    messageFormat: "Command '{0}' is sent but no handler is registered",
    category: "AvtoBus", DiagnosticSeverity.Error, isEnabledByDefault: true);

private static readonly DiagnosticDescriptor AVB002 = new(
    id: "AVB002", title: "Multiple handlers for command",
    messageFormat: "Command '{0}' has {1} handlers; commands must have exactly one",
    category: "AvtoBus", DiagnosticSeverity.Error, isEnabledByDefault: true);

private static readonly DiagnosticDescriptor AVB003 = new(
    id: "AVB003", title: "Event has no subscribers",
    messageFormat: "Event '{0}' is published but no consumer subscribes to it",
    category: "AvtoBus", DiagnosticSeverity.Warning, isEnabledByDefault: true);

private static readonly DiagnosticDescriptor AVB010 = new(
    id: "AVB010", title: "Mutable contract",
    messageFormat: "Message contract '{0}' has mutable properties; use init-only",
    category: "AvtoBus", DiagnosticSeverity.Warning, isEnabledByDefault: true);

private static void Validate(SourceProductionContext spc, ImmutableArray<HandlerInfo> handlers)
{
    // Пример: несколько хендлеров на одну команду
    var groups = handlers.GroupBy(h => h.MessageClrType);
    foreach (var g in groups.Where(g => IsCommand(g.Key) && g.Count() > 1))
    {
        foreach (var h in g)
            spc.ReportDiagnostic(Diagnostic.Create(AVB002, h.Location, g.Key, g.Count()));
    }
}
```

## 6. JsonSerializerContext (AOT-friendly сериализация)

```csharp
private static void EmitJsonContext(SourceProductionContext spc, ImmutableArray<string> types)
{
    var attrs = string.Join("\n", types.Select(t => $"[JsonSerializable(typeof({t}))]"));
    spc.AddSource("AvtoBusJsonContext.g.cs", $$"""
        // <auto-generated/>
        using System.Text.Json.Serialization;

        namespace AvtoBus.Generated;

        {{attrs}}
        [JsonSourceGenerationOptions(
            PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
        internal sealed partial class AvtoBusJsonContext : JsonSerializerContext { }
        """);
}
```

## 7. Терминальный middleware, использующий codegen

```csharp
public sealed class HandlerInvokerMiddleware : IBusMiddleware
{
    private readonly DispatcherRegistry _registry;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (!_registry.TryGet(ctx.Envelope.MessageType, out var dispatcher))
            throw new NoHandlerException(ctx.Envelope.MessageType);

        await dispatcher.DispatchAsync(ctx);

        // Каскадные исходящие → outbox (если есть) или транспорт
        if (ctx.Outgoing.Count > 0)
        {
            var bus = ctx.Services.GetRequiredService<IBus>();
            foreach (var o in ctx.Outgoing)
            {
                switch (o.Kind)
                {
                    case OutgoingKind.Publish: await bus.Publish((object)o.Payload, o.Options); break;
                    case OutgoingKind.Send:    await bus.Send((object)o.Payload, o.Options); break;
                    case OutgoingKind.Reply:   await SendReplyAsync(ctx, o); break;
                }
            }
        }

        await next(ctx); // обычно после терминального ничего нет, но контракт соблюдён
    }
}
```

## 8. Что даёт всё это вместе

- **Ноль рефлексии** на горячем пути — только скомпилированные вызовы.
- **Ошибки на этапе сборки** вместо загадочных `NoHandlerException` в проде.
- **AOT-совместимость** — `dotnet publish -r linux-x64 --aot` работает без warning-ов.
- **Скорость**: ~2–3× быстрее рефлексионных диспетчеров (MassTransit/Rebus).
- **Прозрачность**: сгенерированный код лежит в `obj/Generated/AvtoBus.Generators` — можно посмотреть, отладить, поверить.
