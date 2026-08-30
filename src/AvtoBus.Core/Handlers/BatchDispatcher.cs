using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Handlers;

/// <summary>
/// Вызывает батч-хендлер: одна обработка на N сообщений (идея 19).
/// Тип сообщения — элемент <c>IMessageBatch&lt;T&gt;</c>.
/// </summary>
public interface IBatchDispatcher
{
    Type MessageType { get; }

    string HandlerName { get; }

    /// <summary>
    /// Выполняет батч-хендлер. Возвращает значение, которое хендлер вернул (каскад),
    /// либо <c>null</c>. <paramref name="contexts"/> — контекст каждого сообщения батча,
    /// разделяющих один DI-скоуп.
    /// </summary>
    ValueTask<object?> DispatchAsync(
        IReadOnlyList<object> messages,
        IReadOnlyList<ConsumeContext> contexts,
        IServiceProvider services);
}

/// <summary>Таблица «тип сообщения → его батч-хендлер» (идея 20).</summary>
public sealed class BatchDispatcherRegistry(IEnumerable<IBatchDispatcher> dispatchers)
{
    private readonly FrozenDictionary<Type, IBatchDispatcher> _byType =
        dispatchers.ToFrozenDictionary(d => d.MessageType);

    public IBatchDispatcher? For(Type messageType)
        => _byType.TryGetValue(messageType, out var dispatcher) ? dispatcher : null;

    public bool HasBatchHandlerFor(Type messageType) => _byType.ContainsKey(messageType);

    public IEnumerable<Type> Types => _byType.Keys;
}

/// <summary>Собирает типизированный <see cref="MessageBatch{T}"/> из нетипизированных сообщений.</summary>
internal static class BatchBuilder
{
    public static object Build<T>(IReadOnlyList<object> messages, IReadOnlyList<ConsumeContext> contexts) where T : class
    {
        var typed = new T[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            typed[i] = (T)messages[i];

        return new MessageBatch<T>(typed, contexts);
    }
}

internal static class BatchHandlerBinder
{
    public static bool IsBatchMethod(MethodInfo method)
        => method.GetParameters().Length > 0
           && method.GetParameters()[0].ParameterType.IsGenericType
           && method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IMessageBatch<>);

    /// <summary>Компилирует батч-диспетчер. Reflection-путь (legacy): под AOT батчи не покрываются генератором.</summary>
    [RequiresUnreferencedCode("Компиляция батч-вызова через Expression — reflection-путь (legacy).")]
    public static IBatchDispatcher Bind(MethodInfo method)
    {
        var messageType = method.GetParameters()[0].ParameterType.GetGenericArguments()[0];
        var batchInterface = typeof(IMessageBatch<>).MakeGenericType(messageType);
        var handlerName = $"{method.DeclaringType?.Name}.{method.Name}[batch]";
        var awaiter = ReturnAwaiter.For(method.ReturnType);

        var messagesParam = Expression.Parameter(typeof(IReadOnlyList<object>), "messages");
        var contextsParam = Expression.Parameter(typeof(IReadOnlyList<ConsumeContext>), "contexts");
        var servicesParam = Expression.Parameter(typeof(IServiceProvider), "services");

        var buildBatch = typeof(BatchBuilder)
            .GetMethod(nameof(BatchBuilder.Build), BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        var batchArgument = Expression.Convert(
            Expression.Call(buildBatch, messagesParam, contextsParam),
            batchInterface);

        var argumentCount = method.GetParameters().Length;
        var arguments = new Expression[argumentCount];

        for (var i = 0; i < argumentCount; i++)
        {
            var parameterType = method.GetParameters()[i].ParameterType;

            if (i == 0)
            {
                arguments[0] = batchArgument;
                continue;
            }

            arguments[i] = parameterType switch
            {
                _ when parameterType == typeof(CancellationToken) => Expression.Constant(CancellationToken.None),
                _ when parameterType == typeof(IServiceProvider) => servicesParam,
                _ => Expression.Convert(
                    Expression.Call(
                        GetRequiredMethod,
                        servicesParam,
                        Expression.Constant(parameterType)),
                    parameterType),
            };
        }

        Expression call = method.IsStatic
            ? Expression.Call(method, arguments)
            : Expression.Call(
                Expression.Convert(
                    Expression.Call(
                        GetRequiredMethod,
                        servicesParam,
                        Expression.Constant(method.DeclaringType!)),
                    method.DeclaringType!),
                method,
                arguments);

        Expression body = method.ReturnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));

        var invoker = Expression.Lambda<Func<IReadOnlyList<object>, IReadOnlyList<ConsumeContext>, IServiceProvider, object?>>(
                body, messagesParam, contextsParam, servicesParam)
            .Compile();

        return new MethodBatchDispatcher(messageType, handlerName, invoker, awaiter);
    }

    private static readonly MethodInfo GetRequiredMethod = typeof(BatchHandlerBinder)
        .GetMethod(nameof(GetRequired), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static object GetRequired(IServiceProvider services, Type type)
        => services.GetRequiredService(type);

    private sealed class MethodBatchDispatcher(
        Type messageType,
        string handlerName,
        Func<IReadOnlyList<object>, IReadOnlyList<ConsumeContext>, IServiceProvider, object?> invoker,
        ReturnAwaiter awaiter) : IBatchDispatcher
    {
        public Type MessageType { get; } = messageType;

        public string HandlerName { get; } = handlerName;

        public async ValueTask<object?> DispatchAsync(
            IReadOnlyList<object> messages,
            IReadOnlyList<ConsumeContext> contexts,
            IServiceProvider services)
        {
            var returned = invoker(messages, contexts, services);
            return await awaiter.AwaitAsync(returned).ConfigureAwait(false);
        }
    }
}
