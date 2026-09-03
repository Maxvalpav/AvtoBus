using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Handlers;

/// <summary>
/// Вторая линия обороны (идея 169): хендлер, получающий сообщение, исчерпавшее все ретраи,
/// вместо прямого попадания в DLQ. Может алертить, партиционировать вручную или логировать.
/// </summary>
public interface IFailedConsumerDispatcher
{
    /// <summary>Тип сообщения, которое умеет принимать этот обработчик.</summary>
    Type MessageType { get; }

    string HandlerName { get; }

    ValueTask DispatchAsync(object failed, ConsumeContext context);
}

/// <summary>
/// Строит диспетчеры второй линии обороны. Поддерживаются оба стиля:
/// интерфейс <see cref="IFailedConsumer{T}"/> и метод <c>Handle(IFailed&lt;T&gt; failed, ...)</c>.
/// </summary>
/// <remarks>Reflection-путь (legacy): компиляция через Expression при старте; под AOT
/// вторая линия обороны не покрывается генератором и не должна использоваться.</remarks>
public static class FailedHandlerBinder
{
    private static readonly string[] FailedMethodNames = ["Handle", "HandleAsync", "Consume", "ConsumeAsync"];

    public static bool IsFailedMethod(MethodInfo method)
        => method.GetParameters().Length > 0
           && method.GetParameters()[0].ParameterType.IsGenericType
           && method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IFailed<>);

    [RequiresUnreferencedCode("Компиляция вызова второй линии обороны — reflection-путь (legacy).")]
    [RequiresDynamicCode("Компиляция вызова второй линии обороны через Expression.Compile — reflection-путь (legacy).")]
    public static IFailedConsumerDispatcher BindMethod(MethodInfo method)
    {
        var messageType = method.GetParameters()[0].ParameterType.GetGenericArguments()[0];
        var handlerName = $"{method.DeclaringType?.Name}.{method.Name}[failed]";
        var awaiter = ReturnAwaiter.For(method.ReturnType);

        var invoker = Compile(method, messageType, handlerType: null);
        return new MethodFailedDispatcher(messageType, handlerName, invoker, awaiter);
    }

    [RequiresUnreferencedCode("Компиляция вызова второй линии обороны — reflection-путь (legacy).")]
    [RequiresDynamicCode("Компиляция вызова второй линии обороны через Expression.Compile — reflection-путь (legacy).")]
    public static IFailedConsumerDispatcher BindInterface(Type handlerType, Type messageType, MethodInfo method)
    {
        var handlerName = $"{handlerType.Name}.{method.Name}";
        var awaiter = ReturnAwaiter.For(method.ReturnType);

        var invoker = Compile(method, messageType, handlerType);
        return new InterfaceFailedDispatcher(handlerType, messageType, handlerName, invoker, awaiter);
    }

    [RequiresUnreferencedCode("Построение вызова через Expression — reflection-путь (legacy).")]
    [RequiresDynamicCode("Построение вызова через Expression.Compile — reflection-путь (legacy).")]
    private static Func<object?, object, ConsumeContext, object?> Compile(
        MethodInfo method,
        Type messageType,
        Type? handlerType)
    {
        var handler = Expression.Parameter(typeof(object), "handler");
        var failed = Expression.Parameter(typeof(object), "failed");
        var context = Expression.Parameter(typeof(ConsumeContext), "ctx");
        var failedType = typeof(IFailed<>).MakeGenericType(messageType);

        var parameters = method.GetParameters();
        var arguments = new Expression[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameterType = parameters[i].ParameterType;

            if (i == 0)
            {
                arguments[i] = Expression.Convert(failed, failedType);
                continue;
            }

            arguments[i] = parameterType switch
            {
                _ when parameterType == typeof(ConsumeContext) => context,
                _ when parameterType == typeof(CancellationToken)
                    => Expression.Property(context, nameof(ConsumeContext.CancellationToken)),
                _ when parameterType == typeof(IServiceProvider)
                    => Expression.Property(context, nameof(ConsumeContext.Services)),
                _ => Expression.Convert(
                    Expression.Call(
                        typeof(FailedHandlerBinder).GetMethod(nameof(GetRequired), BindingFlags.NonPublic | BindingFlags.Static)!,
                        Expression.Property(context, nameof(ConsumeContext.Services)),
                        Expression.Constant(parameterType)),
                    parameterType),
            };
        }

        Expression call = method.IsStatic
            ? Expression.Call(method, arguments)
            : Expression.Call(
                Expression.Convert(
                    Expression.Call(
                        typeof(FailedHandlerBinder).GetMethod(nameof(GetRequired), BindingFlags.NonPublic | BindingFlags.Static)!,
                        Expression.Property(context, nameof(ConsumeContext.Services)),
                        Expression.Constant(handlerType!)),
                    handlerType!),
                method,
                arguments);

        Expression body = method.ReturnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));

        return Expression.Lambda<Func<object?, object, ConsumeContext, object?>>(body, handler, failed, context)
            .Compile();
    }

    private static object GetRequired(IServiceProvider services, Type type)
        => services.GetRequiredService(type);

    /// <summary>Диспетчер для классов, реализующих <see cref="IFailedConsumer{T}"/>.</summary>
    private sealed class InterfaceFailedDispatcher(
        Type handlerType,
        Type messageType,
        string handlerName,
        Func<object?, object, ConsumeContext, object?> invoker,
        ReturnAwaiter awaiter) : IFailedConsumerDispatcher
    {
        public Type MessageType { get; } = messageType;

        public string HandlerName { get; } = handlerName;

        public async ValueTask DispatchAsync(object failed, ConsumeContext context)
        {
            var handler = context.Services.GetRequiredService(handlerType);
            var returned = invoker(handler, failed, context);
            await awaiter.AwaitAsync(returned).ConfigureAwait(false);
        }
    }

    /// <summary>Диспетчер для методов по конвенции имени.</summary>
    private sealed class MethodFailedDispatcher(
        Type messageType,
        string handlerName,
        Func<object?, object, ConsumeContext, object?> invoker,
        ReturnAwaiter awaiter) : IFailedConsumerDispatcher
    {
        public Type MessageType { get; } = messageType;

        public string HandlerName { get; } = handlerName;

        public async ValueTask DispatchAsync(object failed, ConsumeContext context)
        {
            var returned = invoker(null, failed, context);
            await awaiter.AwaitAsync(returned).ConfigureAwait(false);
        }
    }
}
