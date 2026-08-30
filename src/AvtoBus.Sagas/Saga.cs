namespace AvtoBus.Sagas;

/// <summary>База состояния саги. Сериализуется хранилищем; Version — для оптимистичной блокировки.</summary>
public abstract class SagaState
{
    public Guid Id { get; set; }
    public int Version { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? Status { get; set; }
}

/// <summary>Сообщение, которое стартует новый инстанс саги.</summary>
public interface IStartedBy<T> where T : class;

/// <summary>Сообщение, которое продолжает существующий инстанс.</summary>
public interface IHandle<T> where T : class;

/// <summary>
/// Всё, что сага может делать: отправлять, публиковать, ждать сообщений (стиль B),
/// планировать таймауты. <see cref="Step{TResult}"/> и <see cref="WaitFor{T}"/> поддерживает
/// только durable-контекст; для стиля A они кидают <see cref="NotSupportedException"/>.
/// </summary>
public interface ISagaContext
{
    IBus Bus { get; }

    ValueTask Send<T>(T cmd) where T : class;

    ValueTask Publish<T>(T evt) where T : class;

    ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class;

    /// <summary>
    /// Детерминированный шаг (стиль B): результат checkpoint-ится в журнале,
    /// при повторе выполняется только то, что впервые (Temporal-style).
    /// </summary>
    ValueTask<TResult> Step<TResult>(Func<Task<TResult>> action, Func<TResult, Task>? compensate = null)
        => throw new NotSupportedException("Step доступен только в durable-саге (стиль B).");

    /// <summary>
    /// Приостановка до следующего сообщения типа <typeparamref name="T"/> (стиль B).
    /// <c>null</c> — таймаут истёк.
    /// </summary>
    ValueTask<T?> WaitFor<T>(TimeSpan? timeout = null) where T : class
        => throw new NotSupportedException("WaitFor доступен только в durable-саге (стиль B).");
}

/// <summary>
/// Стиль A: сага с состоянием (NServiceBus-style, док 17, §1).
/// Состояние восстанавливается из хранилища, оптимистичная блокировка по версии.
/// </summary>
public abstract class Saga<TState> where TState : SagaState, new()
{
    public TState State { get; internal set; } = new();

    /// <summary>Сага завершена: состояние можно удалить из хранилища.</summary>
    public bool IsComplete { get; private set; }

    protected internal ISagaContext Context { get; internal set; } = null!;

    protected void MarkComplete() => IsComplete = true;

    protected ValueTask Send<T>(T cmd) where T : class => Context.Send(cmd);

    protected ValueTask Publish<T>(T evt) where T : class => Context.Publish(evt);

    protected ValueTask RequestTimeout<T>(T timeoutMsg, TimeSpan delay) where T : class
        => Context.RequestTimeoutAsync(timeoutMsg, delay);

    /// <summary>Объявляет, какие сообщения ведут к этому инстансу и с каким ключом.</summary>
    protected virtual void Correlate(SagaMap<TState> map) { }

    /// <summary>Инварианты состояния; проверяются после каждого шага саги.</summary>
    protected virtual void Invariants(SagaInvariants<TState> inv) { }
}

/// <summary>Реализация контекста на базе текущего consume: каскады через outbox текущего сообщения.</summary>
internal sealed class SagaContextImpl(ConsumeContext context) : ISagaContext
{
    public IBus Bus => context.Bus;

    public ValueTask Send<T>(T cmd) where T : class => context.SendAsync(cmd);

    public ValueTask Publish<T>(T evt) where T : class => context.PublishAsync(evt);

    public ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class
        => context.ScheduleAsync(timeoutMsg, delay);
}

/// <summary>Коллекция правил корреляции саги (док 17, §1).</summary>
public sealed class SagaMap<TState> where TState : SagaState
{
    private readonly List<Correlation> _correlations = [];

    /// <summary>Сообщение <typeparamref name="T"/> ведёт к инстансу по ключу, выбранному <paramref name="keySelector"/>.</summary>
    public CorrelationBuilder<TState, T> On<T>(Func<T, object> keySelector) where T : class
    {
        var correlation = new Correlation(typeof(T), message => keySelector((T)message).ToString()!, false);
        _correlations.Add(correlation);
        return new CorrelationBuilder<TState, T>(correlation);
    }

    internal IReadOnlyList<Correlation> Correlations => _correlations;

    internal sealed class Correlation
    {
        public Type MessageType { get; }
        public Func<object, string> Key { get; }
        public bool StartsNew { get; set; }

        public Correlation(Type t, Func<object, string> k, bool s)
            => (MessageType, Key, StartsNew) = (t, k, s);
    }
}

/// <summary>Fluent-донастройка корреляции: помечает сообщение как стартующее новый инстанс.</summary>
public sealed class CorrelationBuilder<TState, T> where TState : SagaState
{
    private readonly SagaMap<TState>.Correlation _correlation;

    internal CorrelationBuilder(SagaMap<TState>.Correlation correlation) => _correlation = correlation;

    public void StartsNew() => _correlation.StartsNew = true;
}

/// <summary>Инварианты состояния: проверяются после каждого шага (док 17, §1).</summary>
public sealed class SagaInvariants<TState> where TState : SagaState
{
    private readonly List<(Func<TState, bool> Predicate, string Name)> _invariants = [];

    public void Assert(Func<TState, bool> predicate, string name)
        => _invariants.Add((predicate, name));

    internal IReadOnlyList<(Func<TState, bool> Predicate, string Name)> Invariants => _invariants;
}

/// <summary>Нарушен инвариант состояния саги.</summary>
public sealed class SagaInvariantViolationException(string name)
    : Exception($"Нарушен инвариант саги: {name}");
