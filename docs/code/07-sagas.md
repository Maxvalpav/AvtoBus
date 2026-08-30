# AvtoBus.Sagas — Реализация саг

> **Code sketch / unverified.** Concurrency, correlation и persistence требуют формальной спецификации и тестов. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus/Saga.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Базовый класс саги с состоянием (NServiceBus-style).
/// Пользователь наследует и переопределяет Handle-методы.
/// </summary>
public abstract class Saga<TState> where TState : SagaState, new()
{
    public TState State { get; internal set; } = new();
    protected bool IsComplete { get; private set; }

    internal ISagaContext Context { get; set; } = null!;

    protected void MarkComplete() => IsComplete = true;

    protected ValueTask Send<T>(T cmd) where T : class
        => Context.Bus.Send(cmd);

    protected ValueTask Publish<T>(T evt) where T : class
        => Context.Bus.Publish(evt);

    protected ValueTask RequestTimeout<T>(T timeoutMsg, TimeSpan delay) where T : class
        => Context.RequestTimeoutAsync(timeoutMsg, delay);

    protected virtual void Correlate(SagaMap<TState> map) { }

    protected virtual void Invariants(SagaInvariants<TState> invariants) { }
}

/// <summary>
/// Состояние саги — хранится в БД и сериализуется.
/// </summary>
public abstract class SagaState
{
    public Guid Id { get; set; }
    public int Version { get; set; }
    public string? Status { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
}

/// <summary>
/// Контекст саги для пользователя.
/// </summary>
public interface ISagaContext
{
    IBus Bus { get; }
    ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class;
}

internal sealed class SagaContextImpl : ISagaContext
{
    public IBus Bus { get; }

    public SagaContextImpl(IBus bus) => Bus = bus;

    public ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class
        => Bus.Schedule(timeoutMsg, delay);
}
```

---

## AvtoBus/SagaMap.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Декларативная корреляция сообщений с инстансом саги.
/// </summary>
public sealed class SagaMap<TState> where TState : SagaState, new()
{
    private readonly List<SagaCorrelation> _correlations = new();

    /// <summary>
    /// Привязать тип сообщения к ключу саги.
    /// </summary>
    public SagaMapBuilder<TState, T> On<T>(Func<T, object> keySelector) where T : class
    {
        var correlation = new SagaCorrelation
        {
            MessageType = typeof(T),
            KeySelector = msg => keySelector((T)msg).ToString()!,
        };
        _correlations.Add(correlation);
        return new SagaMapBuilder<TState, T>(correlation);
    }

    internal IReadOnlyList<SagaCorrelation> Correlations => _correlations;

    internal sealed class SagaCorrelation
    {
        public Type MessageType { get; init; } = null!;
        public Func<object, string> KeySelector { get; init; } = null!;
        public bool StartsNew { get; set; }
    }
}

public sealed class SagaMapBuilder<TState, T> where TState : SagaState, new()
{
    private readonly SagaMap<TState>.SagaCorrelation _correlation;
    internal SagaMapBuilder(SagaMap<TState>.SagaCorrelation correlation) => _correlation = correlation;

    public void StartsNew() => _correlation.StartsNew = true;
}
```

---

## AvtoBus/SagaMetadata.cs

```csharp
using System.Linq.Expressions;
using System.Reflection;

namespace AvtoBus;

/// <summary>
/// Метаданные саги: правила корреляции + SLA.
/// </summary>
public sealed class SagaMetadata
{
    public Type SagaType { get; init; } = null!;
    public Type StateType { get; init; } = null!;
    public IReadOnlyList<SagaCorrelationMetadata> Correlations { get; init; } = Array.Empty<SagaCorrelationMetadata>();

    public TimeSpan? SlaMaxDuration { get; init; }
    public Type? SlaFrom { get; init; }
    public Type? SlaTo { get; init; }

    /// <summary>
    /// Получить правило корреляции для типа сообщения.
    /// </summary>
    public SagaCorrelationMetadata? GetCorrelation(Type messageType)
        => Correlations.FirstOrDefault(c =>
            c.MessageType == messageType || c.MessageType.IsAssignableFrom(messageType));
}

public sealed class SagaCorrelationMetadata
{
    public Type MessageType { get; init; } = null!;
    public Func<object, string> KeySelector { get; init; } = null!;
    public bool StartsNew { get; init; }
}

public sealed class SagaInvariants<TState> where TState : SagaState, new()
{
    private readonly List<Action<TState>> _checks = new();

    public void Assert(Func<TState, bool> predicate, string description)
    {
        _checks.Add(state =>
        {
            if (!predicate(state))
                throw new SagaInvariantViolationException(description, state.Id);
        });
    }

    public void AssertEqual(Func<TState, object?> expected, Func<TState, object?> actual, string description)
    {
        _checks.Add(state =>
        {
            var e = expected(state);
            var a = actual(state);
            if (!Equals(e, a))
                throw new SagaInvariantViolationException(
                    $"{description}: expected={e}, actual={a}", state.Id);
        });
    }

    internal void Validate(TState state)
    {
        foreach (var check in _checks)
            check(state);
    }
}

public sealed class SagaInvariantViolationException : Exception
{
    public Guid SagaInstanceId { get; }
    public SagaInvariantViolationException(string message, Guid instanceId)
        : base($"Saga invariant violated: {message} (instance={instanceId})")
        => SagaInstanceId = instanceId;
}
```

---

## AvtoBus/ISagaStore.cs

```csharp
namespace AvtoBus;

public interface ISagaStore
{
    ValueTask<SagaInstance?> LoadAsync(Type sagaType, string correlationKey, CancellationToken ct);
    ValueTask SaveAsync(Type sagaType, SagaInstance instance, int expectedVersion, CancellationToken ct);
    ValueTask CompleteAsync(Type sagaType, Guid instanceId, CancellationToken ct);
    ValueTask<IReadOnlyList<SagaInstance>> QueryAsync(Type? sagaType = null, string? status = null,
        int skip = 0, int take = 100, CancellationToken ct = default);
    ValueTask<SagaInstance?> GetAsync(Guid instanceId, CancellationToken ct);
}

public sealed class SagaInstance
{
    public Guid Id { get; set; }
    public string SagaType { get; set; } = "";
    public string CorrelationKey { get; set; } = "";
    public string StateJson { get; set; } = "";
    public int Version { get; set; }
    public string? Status { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; }
    public bool IsPaused { get; set; }
}
```

---

## AvtoBus/InMemorySagaStore.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus;

/// <summary>
/// InMemory реализация ISagaStore для тестирования.
/// </summary>
public sealed class InMemorySagaStore : ISagaStore
{
    private readonly ConcurrentDictionary<(string SagaType, string Key), SagaInstance> _instances = new();
    private readonly ConcurrentDictionary<Guid, SagaInstance> _byId = new();

    public ValueTask<SagaInstance?> LoadAsync(Type sagaType, string correlationKey, CancellationToken ct)
    {
        var key = (sagaType.FullName ?? sagaType.Name, correlationKey);
        _instances.TryGetValue(key, out var instance);
        return ValueTask.FromResult(instance);
    }

    public ValueTask SaveAsync(Type sagaType, SagaInstance instance, int expectedVersion, CancellationToken ct)
    {
        if (expectedVersion > 0)
        {
            var existing = _instances.Values.FirstOrDefault(i => i.Id == instance.Id);
            if (existing is not null && existing.Version != expectedVersion)
                throw new SagaConcurrencyException(instance.Id, expectedVersion);
        }

        instance.SagaType = sagaType.FullName ?? sagaType.Name;
        instance.Version++;
        instance.UpdatedAt = DateTime.UtcNow;

        var key = (instance.SagaType, instance.CorrelationKey);
        _instances[key] = instance;
        _byId[instance.Id] = instance;

        return ValueTask.CompletedTask;
    }

    public ValueTask CompleteAsync(Type sagaType, Guid instanceId, CancellationToken ct)
    {
        if (_byId.TryGetValue(instanceId, out var instance))
        {
            instance.Status = "Completed";
            instance.CompletedAt = DateTime.UtcNow;
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<SagaInstance>> QueryAsync(
        Type? sagaType = null, string? status = null,
        int skip = 0, int take = 100, CancellationToken ct = default)
    {
        var query = _instances.Values.AsEnumerable();

        if (sagaType is not null)
            query = query.Where(i => i.SagaType == sagaType.FullName);
        if (status is not null)
            query = query.Where(i => i.Status == status);

        var result = query.Skip(skip).Take(take).ToList();
        return ValueTask.FromResult<IReadOnlyList<SagaInstance>>(result);
    }

    public ValueTask<SagaInstance?> GetAsync(Guid instanceId, CancellationToken ct)
    {
        _byId.TryGetValue(instanceId, out var instance);
        return ValueTask.FromResult(instance);
    }
}

public sealed class SagaConcurrencyException : Exception
{
    public Guid InstanceId { get; }
    public int ExpectedVersion { get; }
    public SagaConcurrencyException(Guid instanceId, int expectedVersion)
        : base($"Concurrency conflict on saga {instanceId}. Expected version {expectedVersion}.")
    {
        InstanceId = instanceId;
        ExpectedVersion = expectedVersion;
    }
}
```

---

## AvtoBus/SagaMiddleware.cs

```csharp
using AvtoBus.Pipeline;

namespace AvtoBus;

/// <summary>
/// Middleware: корреляция сообщения с инстансом саги, загрузка/сохранение, диспетчеризация.
/// </summary>
internal sealed class SagaMiddleware<TSaga, TState> : IBusMiddleware
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly ISagaStore _store;
    private readonly ISagaContextFactory _contextFactory;
    private readonly ILogger<SagaMiddleware<TSaga, TState>> _log;
    private readonly SagaMetadata _metadata;

    public SagaMiddleware(
        ISagaStore store,
        ISagaContextFactory contextFactory,
        ILogger<SagaMiddleware<TSaga, TState>> log)
    {
        _store = store;
        _contextFactory = contextFactory;
        _log = log;
        _metadata = BuildMetadata();
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var messageType = ctx.Message.GetType();
        var correlation = _metadata.GetCorrelation(messageType);

        if (correlation is null)
        {
            await next(ctx);
            return;
        }

        var key = correlation.KeySelector(ctx.Message);

        // 1. Загрузить или создать инстанс
        var existing = await _store.LoadAsync(typeof(TSaga), key, ctx.CancellationToken);

        TSaga saga;
        int expectedVersion;

        if (existing is null)
        {
            if (!correlation.StartsNew)
            {
                _log.LogDebug("Late message for saga key={Key} — ignoring", key);
                return;
            }

            saga = new TSaga
            {
                State = new TState
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow,
                }
            };
            expectedVersion = 0;
            BusMetrics.SagaStarted.Add(1);
            _log.LogInformation("Starting saga {Saga} for key={Key}", typeof(TSaga).Name, key);
        }
        else
        {
            var stateJson = System.Text.Json.JsonSerializer.Deserialize<TState>(existing.StateJson)!;
            stateJson.Id = existing.Id;
            saga = new TSaga { State = stateJson };
            expectedVersion = existing.Version;

            if (existing.IsPaused)
            {
                _log.LogDebug("Saga {Id} is paused — ignoring message", existing.Id);
                return;
            }
        }

        // 2. Связать с контекстом
        var context = _contextFactory.Create(ctx);
        saga.Context = context;

        // 3. Обработать сообщение через диспетчер саги
        var handler = new SagaDispatcher<TSaga, TState>(_metadata);
        await handler.HandleAsync(saga, ctx.Message);

        // 4. Проверить инварианты
        var invariants = new SagaInvariants<TState>();
        saga.Invariants(invariants);
        invariants.Validate(saga.State);

        // 5. Сохранить
        var instance = new SagaInstance
        {
            Id = saga.State.Id,
            CorrelationKey = key,
            StateJson = System.Text.Json.JsonSerializer.Serialize(saga.State),
            Status = saga.IsComplete ? "Completed" : "Active",
        };

        await _store.SaveAsync(typeof(TSaga), instance, expectedVersion, ctx.CancellationToken);

        if (saga.IsComplete)
        {
            await _store.CompleteAsync(typeof(TSaga), saga.State.Id, ctx.CancellationToken);
            BusMetrics.SagaCompleted.Add(1);
            _log.LogInformation("Saga {Saga} {Id} completed", typeof(TSaga).Name, saga.State.Id);
        }

        await next(ctx);
    }

    private SagaMetadata BuildMetadata()
    {
        var tmp = new TSaga();
        var map = new SagaMap<TState>();
        tmp.Correlate(map);

        return new SagaMetadata
        {
            SagaType = typeof(TSaga),
            StateType = typeof(TState),
            Correlations = map.Correlations.Select(c => new SagaCorrelationMetadata
            {
                MessageType = c.MessageType,
                KeySelector = c.KeySelector,
                StartsNew = c.StartsNew,
            }).ToList(),
        };
    }
}
```

---

## AvtoBus/SagaDispatcher.cs

```csharp
using System.Reflection;

namespace AvtoBus;

/// <summary>
/// Диспетчер саги: вызывает нужный Handle-метод по типу сообщения.
/// </summary>
internal sealed class SagaDispatcher<TSaga, TState>
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly SagaMetadata _metadata;
    private readonly Dictionary<Type, MethodInfo> _handleMethods;

    public SagaDispatcher(SagaMetadata metadata)
    {
        _metadata = metadata;
        _handleMethods = typeof(TSaga)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(m => m.Name.StartsWith("Handle") && m.GetParameters().Length >= 1)
            .ToDictionary(m => m.GetParameters()[0].ParameterType, m => m);
    }

    public async Task HandleAsync(TSaga saga, object message)
    {
        var messageType = message.GetType();

        // Ищем Handle с точным типом
        if (!_handleMethods.TryGetValue(messageType, out var method))
        {
            // Ищем по интерфейсам: IHandle<T>, IStartedBy<T>
            foreach (var kv in _handleMethods)
            {
                if (kv.Key.IsAssignableFrom(messageType))
                {
                    method = kv.Value;
                    break;
                }
            }
        }

        if (method is null)
        {
            throw new InvalidOperationException(
                $"Saga {typeof(TSaga).Name} has no handler for {messageType.Name}. " +
                $"Available handlers: {string.Join(", ", _handleMethods.Keys.Select(t => t.Name))}");
        }

        var result = method.Invoke(saga, new[] { message });

        if (result is Task task)
            await task;
    }
}
```

---

## AvtoBus/SagaBuilder.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Fluent builder для конфигурации саги.
/// </summary>
public sealed class SagaBuilder<TSaga, TState>
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly SagaConfiguration _config;

    internal SagaBuilder(SagaConfiguration config) => _config = config;

    public SagaBuilder<TSaga, TState> Sla(
        Type from, Type to, TimeSpan maxDuration)
    {
        _config.SlaFrom = from;
        _config.SlaTo = to;
        _config.SlaMaxDuration = maxDuration;
        return this;
    }

    public SagaBuilder<TSaga, TState> Sla(
        TimeSpan maxDuration)
    {
        _config.SlaMaxDuration = maxDuration;
        return this;
    }
}

internal sealed class SagaConfiguration
{
    public Type SagaType { get; }
    public Type StateType { get; }
    public Type? SlaFrom { get; set; }
    public Type? SlaTo { get; set; }
    public TimeSpan? SlaMaxDuration { get; set; }

    public SagaConfiguration(Type sagaType, Type stateType)
    {
        SagaType = sagaType;
        StateType = stateType;
    }
}
```

---

## AvtoBus/DeadLetterException.cs

```csharp
namespace AvtoBus;

/// <summary>
/// Исключение: отправить сообщение в DLQ без ретрая.
/// </summary>
public sealed class DeadLetterException : Exception
{
    public Envelope Envelope { get; }
    public string Reason { get; }

    public DeadLetterException(Envelope envelope, string reason)
        : base($"Dead-lettered: {envelope.MessageType} — {reason}")
    {
        Envelope = envelope;
        Reason = reason;
    }
}

/// <summary>
/// Исключение: хендлер для команды не найден.
/// </summary>
public sealed class NoHandlerException : Exception
{
    public string MessageType { get; }

    public NoHandlerException(string messageType)
        : base($"No handler registered for message type: {messageType}")
    {
        MessageType = messageType;
    }
}

/// <summary>
/// Исключение: suspend саги (для durable execution).
/// </summary>
internal sealed class SagaSuspendException : Exception { }

/// <summary>
/// Исключение: abort саги (компенсации).
/// </summary>
internal sealed class SagaAbortException : Exception { }
```
