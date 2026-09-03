using System.Linq.Expressions;
using System.Reflection;
using AvtoBus.Handlers;

namespace AvtoBus.Sagas;

/// <summary>
/// Метаданные саги: корреляции из <see cref="Saga{TState}.Correlate"/>, инварианты и
/// диспетчеризация в конкретный <c>Handle(T)</c>. Строятся один раз при регистрации.
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
    "Метаданные строятся сканированием методов саги и компиляцией вызовов через Expression — несовместимо с trimming/AOT.")]
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
    "Диспетчеры саги компилируются через Expression.Compile — несовместимо с NativeAOT.")]
internal sealed class SagaMetadata<
    [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
        System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicMethods |
        System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.NonPublicMethods)] TSaga,
    TState>
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    public static SagaMetadata<TSaga, TState> Build()
    {
        // Correlate — protected virtual; на пустом экземпляре вытаскиваем правила корреляции.
        var map = new SagaMap<TState>();
        InvokeProtected("Correlate", map);

        var invariants = new SagaInvariants<TState>();
        InvokeProtected("Invariants", invariants);

        var correlations = map.Correlations.ToArray();
        return new SagaMetadata<TSaga, TState>(correlations, invariants.Invariants.ToArray());
    }

    private readonly SagaMap<TState>.Correlation[] _correlations;
    private readonly (Func<TState, bool> Predicate, string Name)[] _invariants;
    private readonly Dictionary<Type, Func<TSaga, object, Task>> _invokers;

    private SagaMetadata(
        SagaMap<TState>.Correlation[] correlations,
        (Func<TState, bool>, string)[] invariants)
    {
        _correlations = correlations;
        _invariants = invariants;
        _invokers = BuildInvokers();
    }

    public IReadOnlyList<Type> MessageTypes => _correlations.Select(c => c.MessageType).ToArray();

    internal IReadOnlyList<SagaMap<TState>.Correlation> Correlations => _correlations;

    public Type HandlerType => typeof(TSaga);

    public void CheckInvariants(TState state)
    {
        foreach (var (predicate, name) in _invariants)
        {
            if (!predicate(state))
                throw new SagaInvariantViolationException(name);
        }
    }

    /// <summary>Находит диспетчер для сообщения: по точному типу или наследнику/интерфейсу.</summary>
    public SagaMap<TState>.Correlation? CorrelationFor(object message)
    {
        var type = message.GetType();
        return _correlations.FirstOrDefault(c => c.MessageType.IsAssignableFrom(type));
    }

    public async ValueTask InvokeAsync(TSaga saga, object message)
    {
        await InvokerFor(message)(saga, message).ConfigureAwait(false);
    }

    /// <summary>Компилированный вызов Handle для сообщения (с учётом наследников/интерфейсов).</summary>
    internal Func<TSaga, object, Task> InvokerFor(object message)
        => InvokerForType(message.GetType());

    internal Func<TSaga, object, Task> InvokerForType(Type messageType)
    {
        if (_invokers.TryGetValue(messageType, out var invoker))
            return invoker;

        foreach (var correlated in _correlations.Select(c => c.MessageType))
        {
            if (correlated.IsAssignableFrom(messageType))
                return _invokers[correlated];
        }

        throw new SagaException($"Сага {typeof(TSaga).Name} не имеет обработчика для {messageType.Name}");
    }

    private Dictionary<Type, Func<TSaga, object, Task>> BuildInvokers()
    {
        var result = new Dictionary<Type, Func<TSaga, object, Task>>();

        foreach (var messageType in _correlations.Select(c => c.MessageType))
        {
            var method = FindHandleMethod(messageType)
                         ?? throw new SagaException(
                             $"Сага {typeof(TSaga).Name} не имеет Handle({messageType.Name}) для коррелируемого сообщения");

            result[messageType] = CompileInvoker(method, messageType);
        }

        return result;
    }

    private MethodInfo? FindHandleMethod(Type messageType)
    {
        foreach (var name in new[] { "Handle", "HandleAsync", "Consume", "ConsumeAsync" })
        {
            var method = typeof(TSaga).GetMethod(
                name,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null,
                [messageType],
                null);

            if (method is not null)
                return method;
        }

        return null;
    }

    /// <summary>Компилирует вызов Handle(T): сообщение из контекста, остальное — из DI-скоупа.</summary>
    private static Func<TSaga, object, Task> CompileInvoker(MethodInfo method, Type messageType)
    {
        var saga = Expression.Parameter(typeof(TSaga), "saga");
        var message = Expression.Parameter(typeof(object), "message");

        var call = Expression.Call(
            saga,
            method,
            Expression.Convert(message, messageType));

        // Handle возвращает void/Task/ValueTask — нормализуем всё к Task.
        Expression body = call.Type switch
        {
            _ when call.Type == typeof(Task) => call,
            _ when call.Type == typeof(ValueTask) => Expression.Call(call, nameof(ValueTask.AsTask), null),
            _ when call.Type == typeof(void) => Expression.Block(call, Expression.Constant(Task.CompletedTask)),
            // Task<T> — апкаст к Task (Task<T> : Task).
            _ when call.Type.IsGenericType && call.Type.GetGenericTypeDefinition() == typeof(Task<>)
                => Expression.Convert(call, typeof(Task)),
            // ValueTask<T>.AsTask() → Task<T> → Task.
            _ when call.Type.IsGenericType => Expression.Convert(
                Expression.Call(call, nameof(ValueTask<object>.AsTask), null),
                typeof(Task)),
            _ => Expression.Block(call, Expression.Constant(Task.CompletedTask)),
        };

        return Expression.Lambda<Func<TSaga, object, Task>>(body, saga, message).Compile();
    }

    private static void InvokeProtected(string methodName, object arg)
    {
        var method = typeof(Saga<TState>).GetMethod(
            methodName,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        method!.Invoke(new TSaga(), [arg]);
    }
}

/// <summary>Ошибка конфигурации саги.</summary>
public sealed class SagaException(string message) : Exception(message);
