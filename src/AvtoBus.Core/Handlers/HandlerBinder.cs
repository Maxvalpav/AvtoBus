using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Handlers;

/// <summary>
/// Превращает метод-хендлер в диспетчер. Рефлексия работает ровно один раз — при старте:
/// дальше вызывается скомпилированный делегат, без <c>MethodInfo.Invoke</c> на горячем пути.
/// </summary>
/// <remarks>Reflection-путь (legacy): под AOT методы-хендлеры покрываются генератором,
/// и этот класс не используется вовсе (док 16, §8).</remarks>
public static class HandlerBinder
{
    private static readonly string[] HandlerMethodNames = ["Handle", "HandleAsync", "Consume", "ConsumeAsync"];

    /// <summary>Считается ли метод хендлером по конвенции имени.</summary>
    public static bool IsHandlerMethod(MethodInfo method)
        => HandlerMethodNames.Contains(method.Name, StringComparer.Ordinal)
           && !method.IsSpecialName
           && method.GetParameters().Length > 0;

    /// <summary>
    /// Находит все методы-хендлеры в типе: и статические, и инстансные.
    /// </summary>
    [RequiresUnreferencedCode("GetMethods — reflection-путь (legacy); под AOT генератор покрывает хендлеры.")]
    public static IEnumerable<MethodInfo> FindHandlerMethods(Type type)
        => type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(IsHandlerMethod);

    /// <summary>
    /// Первый параметр метода — это и есть тип сообщения.
    /// Остальные параметры инжектятся из DI (или являются контекстом/токеном).
    /// </summary>
    public static Type MessageTypeOf(MethodInfo method)
    {
        var first = method.GetParameters()[0].ParameterType;

        // ConsumeContext<T> первым параметром — тоже валидная сигнатура.
        if (first.IsGenericType && first.GetGenericTypeDefinition() == typeof(ConsumeContext<>))
            return first.GetGenericArguments()[0];

        if (first.IsGenericType && first.GetGenericTypeDefinition() == typeof(IMessageBatch<>))
            return first.GetGenericArguments()[0];

        if (first.IsGenericType && first.GetGenericTypeDefinition() == typeof(IFailed<>))
            return first.GetGenericArguments()[0];

        return first;
    }

    /// <summary>
    /// Собирает диспетчер: компилирует вызов метода с подстановкой аргументов из
    /// <see cref="ConsumeContext"/> и scoped-контейнера.
    /// </summary>
    [RequiresUnreferencedCode("Компиляция вызова хендлера через Expression — reflection-путь (legacy).")]
    [RequiresDynamicCode("Компиляция вызова хендлера через Expression.Compile — reflection-путь (legacy).")]
    public static IMessageDispatcher Bind(MethodInfo method)
    {
        var messageType = MessageTypeOf(method);
        var handlerName = $"{method.DeclaringType?.Name}.{method.Name}";

        var contextParameter = Expression.Parameter(typeof(ConsumeContext), "ctx");
        var arguments = BuildArguments(method, messageType, contextParameter);

        Expression call = method.IsStatic
            ? Expression.Call(method, arguments)
            : Expression.Call(ResolveHandlerInstance(method.DeclaringType!, contextParameter), method, arguments);

        // Метод возвращает object? — Apply разложит его в каскады. void превращаем в null.
        Expression body = method.ReturnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));

        var invoker = Expression.Lambda<Func<ConsumeContext, object?>>(body, contextParameter).Compile();
        var awaiter = ReturnAwaiter.For(method.ReturnType);

        return new MethodDispatcher(messageType, handlerName, method, invoker, awaiter);
    }

    [RequiresUnreferencedCode("Expression-компиляция резолва хендлера из DI — reflection-путь (legacy).")]
    private static Expression ResolveHandlerInstance(Type declaringType, ParameterExpression contextParameter)
    {
        // Инстансный хендлер резолвится из scope сообщения — как scoped-сервис.
        var services = Expression.Property(contextParameter, nameof(ConsumeContext.Services));
        var resolve = typeof(HandlerBinder).GetMethod(nameof(GetRequired), BindingFlags.NonPublic | BindingFlags.Static)!;
        return Expression.Convert(
            Expression.Call(resolve, services, Expression.Constant(declaringType)),
            declaringType);
    }

    private static object GetRequired(IServiceProvider services, Type type)
        => services.GetRequiredService(type);

    [RequiresUnreferencedCode("Построение аргументов через Expression — reflection-путь (legacy).")]
    private static Expression[] BuildArguments(
        MethodInfo method,
        Type messageType,
        ParameterExpression contextParameter)
    {
        var parameters = method.GetParameters();
        var arguments = new Expression[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameterType = parameters[i].ParameterType;

            if (i == 0)
            {
                arguments[i] = FirstArgument(parameterType, messageType, contextParameter);
                continue;
            }

            arguments[i] = parameterType switch
            {
                _ when parameterType == typeof(CancellationToken)
                    => Expression.Property(contextParameter, nameof(ConsumeContext.CancellationToken)),

                _ when parameterType == typeof(ConsumeContext)
                    => contextParameter,

                _ when parameterType == typeof(IServiceProvider)
                    => Expression.Property(contextParameter, nameof(ConsumeContext.Services)),

                _ when parameterType == typeof(Envelope)
                    => Expression.Property(contextParameter, nameof(ConsumeContext.Envelope)),

                // Всё прочее — зависимость из scoped-контейнера.
                _ => Expression.Convert(
                    Expression.Call(
                        typeof(HandlerBinder).GetMethod(nameof(GetRequired), BindingFlags.NonPublic | BindingFlags.Static)!,
                        Expression.Property(contextParameter, nameof(ConsumeContext.Services)),
                        Expression.Constant(parameterType)),
                    parameterType),
            };
        }

        return arguments;
    }

    [RequiresUnreferencedCode("Построение первого аргумента через Expression — reflection-путь (legacy).")]
    private static Expression FirstArgument(
        Type parameterType,
        Type messageType,
        ParameterExpression contextParameter)
    {
        // Хендлер просит типизированный контекст — отдаём сам контекст, он уже нужного типа.
        if (parameterType.IsGenericType
            && parameterType.GetGenericTypeDefinition() == typeof(ConsumeContext<>))
            return Expression.Convert(contextParameter, parameterType);

        // Хендлер просит само сообщение — достаём из контекста и приводим.
        var message = Expression.Property(contextParameter, nameof(ConsumeContext.Message));
        return Expression.Convert(message, messageType);
    }

    /// <summary>Диспетчер поверх скомпилированного вызова метода-хендлера.</summary>
    private sealed class MethodDispatcher(
        Type messageType,
        string handlerName,
        MethodInfo method,
        Func<ConsumeContext, object?> invoker,
        ReturnAwaiter awaiter) : IMessageDispatcher, IHandlerTimeoutProvider, IHandlerAuthorizationProvider
    {
        public Type MessageType { get; } = messageType;

        public string HandlerName { get; } = handlerName;

        public TimeSpan? Timeout
            => method.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout
               ?? method.DeclaringType?.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout;

        public BusAuthorizeAttribute? Authorization
            => method.GetCustomAttribute<BusAuthorizeAttribute>()
               ?? method.DeclaringType?.GetCustomAttribute<BusAuthorizeAttribute>();

        public async ValueTask DispatchAsync(ConsumeContext context)
        {
            var returned = invoker(context);
            var cascade = await awaiter.AwaitAsync(returned).ConfigureAwait(false);
            HandlerResult.Apply(context, cascade);
        }
    }
}
