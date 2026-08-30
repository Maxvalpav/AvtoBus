using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace AvtoBus.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class HandlerGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 1. Методы-хендлеры по конвенции имени (Handle/Consume).
        var methods = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is MethodDeclarationSyntax,
                transform: static (ctx, ct) => ExtractMethodHandler(ctx, ct))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!);

        // 2. Классы, реализующие IConsumer<T>.
        var interfaceConsumers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is ClassDeclarationSyntax,
                transform: static (ctx, ct) => ExtractInterfaceConsumer(ctx, ct))
            .Where(static c => c is not null)
            .Select(static (c, _) => c!);

        // 3. Хендлеры по конвенции, чей первый параметр заведомо не контракт — AVB008.
        var badHandlers = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is MethodDeclarationSyntax,
                transform: static (ctx, ct) => ExtractBadMethodHandler(ctx, ct))
            .Where(static b => b is not null)
            .Select(static (b, _) => b!)
            .Collect();

        // 3. Компиляция + опции для эмиттера и диагностик.
        var compilation = context.CompilationProvider;

        // 4. Эмиссия диспетчеров для методов.
        context.RegisterSourceOutput(
            methods.Combine(compilation),
            static (spc, pair) => Emitters.EmitDispatcher(spc, pair.Left, pair.Right));

        // 5. Эмиссия диспетчеров для IConsumer<T>.
        context.RegisterSourceOutput(
            interfaceConsumers.Combine(compilation),
            static (spc, pair) => Emitters.EmitInterfaceDispatcher(spc, pair.Left, pair.Right));

        // 6. Единый ModuleInitializer с регистрацией всех диспетчеров сборки.
        var all = methods.Collect().Combine(interfaceConsumers.Collect());
        context.RegisterSourceOutput(
            all.Combine(compilation),
            static (spc, pair) => Emitters.EmitRegistration(spc, pair.Left.Left, pair.Left.Right));

        // 7. Вызовы Send/Publish в пользовательском коде — для AVB003.
        var invocations = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is InvocationExpressionSyntax,
                transform: static (ctx, ct) => ExtractInvocation(ctx, ct))
            .Where(static i => i is not null)
            .Select(static (i, _) => i!)
            .Collect();

        // 8. Диагностики.
        context.RegisterSourceOutput(
            all.Combine(compilation),
            static (spc, pair) => Diagnostics.Validate(spc, pair.Left.Left, pair.Left.Right, pair.Right));

        context.RegisterSourceOutput(
            badHandlers.Combine(compilation),
            static (spc, pair) => Diagnostics.ValidateBadHandlers(spc, pair.Left));

        context.RegisterSourceOutput(
            invocations.Combine(all),
            static (spc, pair) => Diagnostics.ValidateInvocations(spc, pair.Left, pair.Right.Left, pair.Right.Right));
    }

    private static InvocationInfo? ExtractInvocation(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var invocation = (InvocationExpressionSyntax)ctx.Node;
        if (ctx.SemanticModel.GetSymbolInfo(invocation, ct).Symbol is not IMethodSymbol method)
            return null;

        var name = method.Name;
        if (name is not ("PublishAsync" or "Publish" or "SendAsync" or "Send"))
            return null;

        if (!method.ContainingType.ToDisplayString().StartsWith("AvtoBus.", StringComparison.Ordinal))
            return null;

        if (method.TypeArguments.Length == 0)
            return null;

        var messageType = method.TypeArguments[0];
        if (messageType is { TypeKind: TypeKind.Interface or TypeKind.TypeParameter })
            return null;

        var isEvent = name.StartsWith("Publish", StringComparison.Ordinal);
        return new InvocationInfo(
            messageType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            isEvent,
            invocation.GetLocation());
    }

    // ---- Извлечение ----------------------------------------------------

    private static HandlerInfo? ExtractMethodHandler(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var methodSyntax = (MethodDeclarationSyntax)ctx.Node;
        if (methodSyntax.Identifier.ValueText is not ("Handle" or "Consume" or "HandleAsync" or "ConsumeAsync"))
            return null;

        var semanticModel = ctx.SemanticModel;
        if (semanticModel.GetDeclaredSymbol(methodSyntax, ct) is not IMethodSymbol method)
            return null;

        if (method.MethodKind != MethodKind.Ordinary)
            return null;

        if (method.Parameters.Length == 0)
            return null;

        // Реализация IConsumer<T>.ConsumeAsync обрабатывается отдельным провайдером — не дублируем.
        if (method.Name == "ConsumeAsync"
            && method.ContainingType.AllInterfaces.Any(static i =>
                i.IsGenericType && i.ConstructedFrom.ToDisplayString() == "AvtoBus.IConsumer<T>"))
            return null;

        if (!method.IsStatic && !method.ContainingType.IsSealed && method.ContainingType.IsAbstract)
            return null;

        var first = method.Parameters[0];
        if (!LooksLikeMessage(first.Type))
            return null;

        // Параметр выглядит конкретным типом, но это заведомо не контракт (string, int, Guid, enum…):
        // согласуется с runtime-фильтром BusConfigurator.IsPlausibleMessageType и даёт AVB008.
        if (IsNonContractFirstParam(first.Type))
            return null;

        // ConsumeContext<T> первым параметром — тоже валидная сигнатура.
        var messageType = first.Type;
        if (messageType is INamedTypeSymbol { IsGenericType: true } generic
            && generic.ConstructedFrom.ToDisplayString() == "AvtoBus.ConsumeContext<T>")
        {
            messageType = generic.TypeArguments[0];
        }

        // Полиморфные параметры (например IOrderEvent) разрешать на этапе компиляции нельзя —
        // они могут быть отправлены из другой сборки. Оставляем только конкретные типы.
        if (messageType.TypeKind is TypeKind.Interface)
            return null;

        var containing = method.ContainingType;
        if (containing.TypeKind != TypeKind.Class && containing.TypeKind != TypeKind.Struct)
            return null;

        var dependencies = method.Parameters.Skip(1)
            .Select(static p => new DependencyInfo(p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), p.Name))
            .ToImmutableArray();

        var returnKind = ClassifyReturn(method.ReturnType);

        return new HandlerInfo(
            ContainingType: containing.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            MethodName: method.Name,
            IsStatic: method.IsStatic,
            IsAsync: method.IsAsync || returnKind is ReturnKind.AwaitVoid or ReturnKind.AwaitValue,
            MessageClrType: messageType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            MessageType: messageType.ToDisplayString(),
            ReturnKind: returnKind,
            IsCommand: ImplementsInterface(messageType, "AvtoBus.ICommand"),
            Dependencies: dependencies,
            Location: methodSyntax.GetLocation(),
            HandlerName: $"{containing.Name}.{method.Name}");
    }

    private static InterfaceConsumerInfo? ExtractInterfaceConsumer(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var classSyntax = (ClassDeclarationSyntax)ctx.Node;
        var semanticModel = ctx.SemanticModel;
        if (semanticModel.GetDeclaredSymbol(classSyntax, ct) is not INamedTypeSymbol type)
            return null;

        if (type.IsAbstract)
            return null;

        foreach (var @interface in type.AllInterfaces)
        {
            if (!@interface.IsGenericType)
                continue;

            if (@interface.ConstructedFrom.ToDisplayString() != "AvtoBus.IConsumer<T>")
                continue;

            var messageType = @interface.TypeArguments[0];
            if (messageType is { TypeKind: TypeKind.Interface or TypeKind.TypeParameter })
                return null;

            return new InterfaceConsumerInfo(
                HandlerType: type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                MessageClrType: messageType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                MessageType: messageType.ToDisplayString(),
                IsCommand: ImplementsInterface(messageType, "AvtoBus.ICommand"),
                Location: classSyntax.GetLocation(),
                HandlerName: $"{type.Name}.Consume");
        }

        return null;
    }

    private static bool ImplementsInterface(ITypeSymbol type, string interfaceName)
    {
        if (type.AllInterfaces.Any(i => i.ToDisplayString() == interfaceName))
            return true;

        return type is INamedTypeSymbol { IsGenericType: true } named
               && named.TypeArguments.Any(t => ImplementsInterface(t, interfaceName));
    }

    private static bool LooksLikeMessage(ITypeSymbol type)
        => type.TypeKind is TypeKind.Class or TypeKind.Struct;

    /// <summary>
    /// Заведомо не-контракт: примитивы, string, object, decimal, DateTime, Guid, enum, массив.
    /// Зеркалит <c>BusConfigurator.IsPlausibleMessageType</c> (reflection-путь), чтобы сгенерированный
    /// и рефлексивный пути не расходились: <c>Handle(string)</c> не должен стать хендлером ни там, ни там.
    /// </summary>
    private static bool IsNonContractFirstParam(ITypeSymbol type)
    {
        if (type.SpecialType != SpecialType.None)
            return true; // string, object, decimal, DateTime, все примитивы

        if (type.TypeKind is TypeKind.Enum or TypeKind.Array or TypeKind.Pointer)
            return true;

        return type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) == "global::System.Guid";
    }

    /// <summary>Метод-хендлер по конвенции с заведомо не-контрактным первым параметром — для AVB008.</summary>
    private static BadHandlerInfo? ExtractBadMethodHandler(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        var methodSyntax = (MethodDeclarationSyntax)ctx.Node;
        if (methodSyntax.Identifier.ValueText is not ("Handle" or "Consume" or "HandleAsync" or "ConsumeAsync"))
            return null;

        if (ctx.SemanticModel.GetDeclaredSymbol(methodSyntax, ct) is not IMethodSymbol method)
            return null;

        if (method.MethodKind != MethodKind.Ordinary)
            return null;

        if (method.Parameters.Length == 0)
            return null;

        var first = method.Parameters[0];

        // Интерфейсный параметр — намеренный отказ (полиморфизм нельзя разрешить на этапе компиляции),
        // а не ошибка контракта: его не флагуем.
        if (first.Type.TypeKind is TypeKind.Interface or TypeKind.TypeParameter)
            return null;

        if (!IsNonContractFirstParam(first.Type))
            return null;

        var location = first.Type.Locations.FirstOrDefault() ?? methodSyntax.GetLocation();
        return new BadHandlerInfo(
            first.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            location);
    }

    /// <summary>
    /// Классифицирует тип возврата по символу (не по display-строке): display-строка
    /// зависит от того, разрешился ли тип в компиляции, а этот классификатор стабилен
    /// и в реальных сборках, и в test-harness без BCL-ссылок.
    /// </summary>
    private static ReturnKind ClassifyReturn(ITypeSymbol returnType)
    {
        if (returnType.SpecialType == SpecialType.System_Void)
            return ReturnKind.Void;

        if (returnType is INamedTypeSymbol named)
        {
            // Task<T> / ValueTask<T> — await + каскад результата.
            if (named.IsGenericType && named.ConstructedFrom.Name is "Task" or "ValueTask")
                return ReturnKind.AwaitValue;

            // Task / ValueTask (в т.ч. unresolved IErrorTypeSymbol в тестах) — await без каскада.
            if (named.Name is "Task" or "ValueTask")
                return ReturnKind.AwaitVoid;
        }

        // Синхронный метод или обычный контракт — результат сразу каскад.
        return ReturnKind.SyncValue;
    }
}
