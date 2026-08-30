using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace AvtoBus.Sagas;

/// <summary>Сагу приостановили до следующего сообщения — ловится раннером (стиль B).</summary>
public sealed class SagaSuspendException : Exception;

/// <summary>Сага абортирована: раннер выполняет компенсации в обратном порядке (стиль B).</summary>
public sealed class SagaAbortException(string reason) : Exception(reason);

/// <summary>Одна запись журнала саги (стиль B). Каждая запись детерминированна и переживает перезапуск.</summary>
public abstract record JournalRecord;

/// <summary>Завершённый шаг: результат сериализован, действие при повторе не выполняется.</summary>
public sealed record StepRecord(string ResultJson, string ResultType) : JournalRecord;

/// <summary>Ожидание сообщения: до получения Payload ноль — сага приостановлена.</summary>
public sealed record WaitRecord(string ExpectedType, TimeSpan? Timeout, string? Payload) : JournalRecord
{
    public bool IsPending => Payload is null;
}

/// <summary>Журнал одной саги: записи шагов/ожиданий, компенсации и исход.</summary>
public sealed class SagaJournal
{
    private readonly List<JournalRecord> _records = [];
    private readonly List<Func<Task>> _compensations = [];

    public string CorrelationKey { get; }

    public Type SagaType { get; }

    /// <summary>Первое сообщение, стартовавшее сагу. Используется для повторного запуска entrypoint.</summary>
    public object? TriggerObject { get; internal set; }

    public SagaOutcome Outcome { get; private set; } = SagaOutcome.Running;

    public DateTime CreatedAt { get; } = DateTime.UtcNow;

    public DateTime UpdatedAt { get; internal set; } = DateTime.UtcNow;

    /// <summary>Записи шагов и ожиданий — ведёт курсор replay-контекста.</summary>
    public IReadOnlyList<JournalRecord> Records => _records;

    public SagaJournal(Type sagaType, string correlationKey, object? triggerObject)
        => (SagaType, CorrelationKey, TriggerObject) = (sagaType, correlationKey, triggerObject);

    internal void Append(JournalRecord record) => _records.Add(record);

    /// <summary>Мутируемый список записей — для replay-контекста и привязки входящих к WaitRecord.</summary>
    internal List<JournalRecord> RecordsInternal => _records;

    internal void UpdateDirty(JournalRecord? record = null)
    {
        if (record is not null)
            Append(record);

        UpdatedAt = DateTime.UtcNow;
    }

    internal void MarkSuspended() => Outcome = SagaOutcome.Suspended;

    internal void RegisterCompensation(Func<Task> compensate) => _compensations.Add(compensate);

    internal void MarkComplete() => Outcome = SagaOutcome.Completed;

    internal void MarkAborted() => Outcome = SagaOutcome.Aborted;

    /// <summary>Выполняет компенсации зарегистрированных шагов в обратном порядке.</summary>
    internal async Task RunCompensationsAsync()
    {
        for (var i = _compensations.Count - 1; i >= 0; i--)
            await _compensations[i]().ConfigureAwait(false);

        _compensations.Clear();
    }
}

public enum SagaOutcome
{
    Running,
    Suspended,
    Completed,
    Aborted,
}

/// <summary>Хранилище журналов durable-саг (стиль B, §5).</summary>
public interface ISagaJournalStore
{
    ValueTask<SagaJournal> LoadOrCreateAsync(Type sagaType, string correlationKey, object? triggerObject, CancellationToken ct);

    ValueTask SaveAsync(SagaJournal journal, CancellationToken ct);
}

/// <summary>In-memory журнал — для тестов и монолита.</summary>
public sealed class InMemorySagaJournalStore : ISagaJournalStore
{
    private readonly ConcurrentDictionary<(Type SagaType, string Key), SagaJournal> _journals = new();

    public ValueTask<SagaJournal> LoadOrCreateAsync(Type sagaType, string correlationKey, object? triggerObject, CancellationToken ct)
    {
        var journal = _journals.GetOrAdd(
            (sagaType, correlationKey),
            _ => new SagaJournal(sagaType, correlationKey, triggerObject));

        return ValueTask.FromResult(journal);
    }

    public ValueTask SaveAsync(SagaJournal journal, CancellationToken ct) => ValueTask.CompletedTask;
}

/// <summary>
/// Durable-контекст: replay-safe шаги (Step) и ожидания (WaitFor). Каждый вызов сперва
/// смотрит в журнал — при повторе действие не выполняется, берётся сохранённый результат.
/// </summary>
public sealed class DurableSagaContext : ISagaContext
{
    private readonly SagaJournal _journal;
    private readonly IBus _bus;
    private readonly ConsumeContext? _consume;
    private int _cursor;

    public DurableSagaContext(SagaJournal journal, IBus bus, ConsumeContext? consume = null)
        => (_journal, _bus, _consume) = (journal, bus, consume);

    public IBus Bus => _bus;

    /// <summary>Отправка через cascade (если есть контекст consume) или напрямую в шину.</summary>
    public ValueTask Send<T>(T cmd) where T : class
        => _consume is not null ? _consume.SendAsync(cmd) : _bus.SendAsync(cmd);

    public ValueTask Publish<T>(T evt) where T : class
        => _consume is not null ? _consume.PublishAsync(evt) : _bus.PublishAsync(evt);

    public async ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class
    {
        if (_consume is not null)
        {
            await _consume.ScheduleAsync(timeoutMsg, delay);
            return;
        }

        await _bus.ScheduleAsync(timeoutMsg, DateTimeOffset.UtcNow + delay);
    }

    public async ValueTask<TResult> Step<TResult>(Func<Task<TResult>> action, Func<TResult, Task>? compensate = null)
    {
        // Replay: результат уже в журнале — действие не выполняется.
        if (_cursor < _journal.Records.Count)
        {
            if (_journal.Records[_cursor] is not StepRecord replayed)
                throw new SagaException($"Журнал саги повреждён: ожидался шаг, найден {_journal.Records[_cursor].GetType().Name}");

            _cursor++;
            return JsonSerializer.Deserialize<TResult>(replayed.ResultJson)!;
        }

        var result = await action().ConfigureAwait(false);
        _journal.Append(new StepRecord(JsonSerializer.Serialize(result), typeof(TResult).FullName!));
        _cursor++;

        if (compensate is not null)
            _journal.RegisterCompensation(() => compensate(result));

        return result;
    }

    public async ValueTask<T?> WaitFor<T>(TimeSpan? timeout = null) where T : class
    {
        // Replay: ожидание уже состоялось (или приостановлено) — берём сохранённый Payload.
        if (_cursor < _journal.Records.Count)
        {
            if (_journal.Records[_cursor] is not WaitRecord replayed)
                throw new SagaException($"Журнал саги повреждён: ожидалось ожидание, найден {_journal.Records[_cursor].GetType().Name}");

            _cursor++;
            return replayed.Payload is null ? null : JsonSerializer.Deserialize<T>(replayed.Payload);
        }

        // Новое ожидание: сага приостанавливается до получения T.
        _journal.Append(new WaitRecord(typeof(T).FullName!, timeout, Payload: null));
        throw new SagaSuspendException();
    }
}

/// <summary>Маркирует класс как durable-сагу (стиль B). CorrelationBy — имя свойства ключа корреляции.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class DurableSagaAttribute : Attribute
{
    /// <summary>Имя свойства, по которому коррелируются сообщения саги (есть у всех её сообщений).</summary>
    public string CorrelationBy { get; set; } = "";
}

/// <summary>
/// Каталог durable-саг: entrypoint (статический Run/Execute(trigger, ISagaContext)) и
/// акссессор ключа корреляции. Строится один раз, кэшируется по (sagaType, messageType).
/// </summary>
public static class SagaCatalog
{
    private static readonly ConcurrentDictionary<Type, Entrypoint> Entrypoints = new();
    private static readonly ConcurrentDictionary<(Type Saga, Type Message), Func<object, string>> KeyAccessors = new();

    private delegate Task Entrypoint(object message, ISagaContext ctx);

    /// <summary>Компилирует вызов статического <c>Run/Execute(trigger, ISagaContext)</c>.</summary>
    public static Func<object, ISagaContext, Task> EntrypointFor(Type sagaType, Type? triggerMessageType = null)
    {
        var target = triggerMessageType ?? TriggerType(sagaType);
        return Entrypoints.GetOrAdd(target, _ => BuildEntrypoint(sagaType, target)).Invoke;
    }

    /// <summary>Тип триггера саги — первый параметр статического Run/Execute.</summary>
    public static Type TriggerType(Type sagaType) => InferTriggerType(sagaType);

    /// <summary>Ключ корреляции для сообщения (свойство из [DurableSaga(CorrelationBy=...)]).</summary>
    public static Func<object, string> KeyAccessorFor(Type sagaType, Type messageType)
        => KeyAccessors.GetOrAdd((sagaType, messageType), _ => BuildKeyAccessor(sagaType, messageType));

    /// <summary>Выводит тип триггера по первому параметру Run/Execute. Используется при самодиагностике.</summary>
    private static Type InferTriggerType(Type sagaType)
    {
        var method = sagaType.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(m =>
                (m.Name is "Run" or "Execute")
                && m.GetParameters() is [var p0, var p1]
                && typeof(ISagaContext).IsAssignableFrom(p1.ParameterType));

        if (method is null)
            throw new SagaException($"Durable-сага {sagaType.Name} не имеет статического Run(trigger, ISagaContext)");

        return method.GetParameters()[0].ParameterType;
    }

    private static Entrypoint BuildEntrypoint(Type sagaType, Type messageType)
    {
        var method = sagaType.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(m =>
                (m.Name is "Run" or "Execute")
                && m.GetParameters() is [var p0, var p1]
                && p0.ParameterType.IsAssignableFrom(messageType)
                && typeof(ISagaContext).IsAssignableFrom(p1.ParameterType));

        if (method is null)
            throw new SagaException($"Durable-сага {sagaType.Name} не имеет статического Run({messageType.Name}, ISagaContext)");

        var message = Expression.Parameter(typeof(object), "message");
        var ctx = Expression.Parameter(typeof(ISagaContext), "ctx");
        var call = Expression.Call(method, Expression.Convert(message, method.GetParameters()[0].ParameterType), ctx);

        Expression body = call.Type switch
        {
            _ when call.Type == typeof(Task) => call,
            _ when call.Type == typeof(ValueTask) => Expression.Call(call, nameof(ValueTask.AsTask), null),
            _ when call.Type == typeof(void) => Expression.Block(call, Expression.Constant(Task.CompletedTask)),
            _ => Expression.Convert(call, typeof(Task)),
        };

        var func = Expression.Lambda<Func<object, ISagaContext, Task>>(body, message, ctx).Compile();
        return (message, context) => func(message, context);
    }

    private static Func<object, string> BuildKeyAccessor(Type sagaType, Type messageType)
    {
        var attribute = sagaType.GetCustomAttribute<DurableSagaAttribute>()
                        ?? throw new SagaException($"Durable-сага {sagaType.Name} не помечена [DurableSaga(CorrelationBy=...)]");

        var property = messageType.GetProperty(attribute.CorrelationBy, BindingFlags.Instance | BindingFlags.Public)
                       ?? throw new SagaException(
                           $"Сообщение {messageType.Name} не имеет свойства '{attribute.CorrelationBy}' для корреляции");

        var message = Expression.Parameter(typeof(object), "message");
        var getter = Expression.Property(Expression.Convert(message, messageType), property);

        // property может быть Guid/string — приводим к строке через ToString().
        var toString = typeof(object).GetMethod(nameof(ToString), BindingFlags.Public | BindingFlags.Instance)!;
        Expression body = property.PropertyType == typeof(string)
            ? getter
            : Expression.Call(Expression.Convert(getter, typeof(object)), toString);

        return Expression.Lambda<Func<object, string>>(body, message).Compile();
    }
}

/// <summary>
/// Раннер durable-саги (стиль B, §5): загружает журнал, привязывает входящее сообщение
/// к ожиданию, реплеит сагу из журнала. Suspend — ждём следующего; Abort — компенсации.
/// </summary>
public sealed class DurableSagaRunner
{
    private readonly ISagaJournalStore _store;
    private readonly IBus _bus;

    public DurableSagaRunner(ISagaJournalStore store, IBus bus)
        => (_store, _bus) = (store, bus);

    public async Task<SagaOutcome> DispatchAsync(Type sagaType, object message, string correlationKey, ConsumeContext? consume = null)
    {
        var journal = await _store.LoadOrCreateAsync(sagaType, correlationKey, message, CancellationToken.None)
            .ConfigureAwait(false);

        // Первый пришедший объект становится триггером; повторные запуски реплеют его же.
        var trigger = journal.TriggerObject ?? message;
        if (journal.TriggerObject is null)
            journal.TriggerObject = trigger;

        // Привязка входящего сообщения к ближайшему ожидающему WaitRecord того же типа.
        BindIncoming(journal, message, consume?.CancellationToken ?? CancellationToken.None);

        // Replay саги с начала: шаги воспроизводятся из журнала, новые выполняются.
        var ctx = new DurableSagaContext(journal, _bus, consume);
        var entrypoint = SagaCatalog.EntrypointFor(sagaType);

        try
        {
            // Replay всегда с триггера: входящее сообщение уже привязано к WaitRecord.
            await entrypoint(trigger, ctx).ConfigureAwait(false);
            journal.MarkComplete();
        }
        catch (SagaSuspendException)
        {
            journal.MarkSuspended();
        }
        catch (SagaAbortException)
        {
            await journal.RunCompensationsAsync().ConfigureAwait(false);
            journal.MarkAborted();
        }

        await _store.SaveAsync(journal, CancellationToken.None).ConfigureAwait(false);
        return journal.Outcome;
    }

    /// <summary>Если есть ожидающее WaitRecord типа сообщения — кладём в него payload из сообщения.</summary>
    private void BindIncoming(SagaJournal journal, object message, CancellationToken ct)
    {
        var messageFullName = message.GetType().FullName;
        for (var i = 0; i < journal.Records.Count; i++)
        {
            if (journal.Records[i] is WaitRecord wait && wait.IsPending
                && wait.ExpectedType == messageFullName)
            {
                journal.RecordsInternal[i] = wait with { Payload = JsonSerializer.Serialize(message) };
                return;
            }
        }
    }
}
