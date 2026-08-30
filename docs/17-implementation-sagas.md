# 🔧 Реализация: Sagas и Durable Execution

> **Design draft.** State-based saga и durable execution являются разными моделями; до реализации их contracts и persistence должны быть разведены по отдельным модулям.

Пакет `AvtoBus.Sagas` — два стиля саг + durable-execution движок в стиле Temporal.

## 1. Стиль A: Saga с состоянием (NServiceBus-style)

```csharp
public abstract class Saga<TState> where TState : SagaState, new()
{
    public TState State { get; internal set; } = new();
    protected bool IsComplete { get; private set; }

    protected internal ISagaContext Context { get; internal set; } = null!;

    protected void MarkComplete() => IsComplete = true;

    protected ValueTask Send<T>(T cmd) where T : class => Context.Bus.Send(cmd);
    protected ValueTask Publish<T>(T evt) where T : class => Context.Bus.Publish(evt);

    protected ValueTask RequestTimeout<T>(T timeoutMsg, TimeSpan delay) where T : class
        => Context.RequestTimeoutAsync(timeoutMsg, delay);

    protected virtual void Correlate(SagaMap<TState> map) { }
    protected virtual void Invariants(SagaInvariants<TState> inv) { }
}

public abstract class SagaState
{
    public Guid Id { get; set; }
    public int Version { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? Status { get; set; }
}

public sealed class SagaMap<TState>
{
    private readonly List<Correlation> _correlations = new();

    public CorrelationBuilder<TState, T> On<T>(Func<T, object> keySelector) where T : class
    {
        var c = new Correlation(typeof(T), m => keySelector((T)m).ToString()!, false);
        _correlations.Add(c);
        return new CorrelationBuilder<TState, T>(c);
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

public sealed class CorrelationBuilder<TState, T>
{
    private readonly SagaMap<TState>.Correlation _c;
    internal CorrelationBuilder(SagaMap<TState>.Correlation c) => _c = c;
    public void StartsNew() => _c.StartsNew = true;
}
```

Пример пользовательской саги:

```csharp
public sealed class OrderSagaState : SagaState
{
    public Guid OrderId { get; set; }
    public decimal Total { get; set; }
    public bool Paid { get; set; }
    public bool Shipped { get; set; }
}

public sealed class OrderSaga : Saga<OrderSagaState>,
    IStartedBy<OrderPlaced>, IHandle<PaymentCompleted>, IHandle<ShipmentDispatched>
{
    protected override void Correlate(SagaMap<OrderSagaState> map)
    {
        map.On<OrderPlaced>(m => m.OrderId).StartsNew();
        map.On<PaymentCompleted>(m => m.OrderId);
        map.On<ShipmentDispatched>(m => m.OrderId);
    }

    public Task Handle(OrderPlaced m)
    {
        State.OrderId = m.OrderId;
        State.Total = m.Total;
        State.Status = "AwaitingPayment";
        return Send(new RequestPayment(m.OrderId, m.Total)).AsTask();
    }

    public Task Handle(PaymentCompleted m)
    {
        State.Paid = true;
        State.Status = "Shipping";
        return Send(new CreateShipment(State.OrderId)).AsTask();
    }

    public Task Handle(ShipmentDispatched m)
    {
        State.Shipped = true;
        State.Status = "Done";
        MarkComplete();
        return Publish(new OrderFulfilled(State.OrderId)).AsTask();
    }

    protected override void Invariants(SagaInvariants<OrderSagaState> inv)
        => inv.Assert(s => s.Total >= 0, "negative-total");
}
```

## 2. ISagaStore + optimistic concurrency

```csharp
public interface ISagaStore
{
    ValueTask<(TState state, int version)?> LoadAsync<TState>(Type sagaType, string correlationKey,
        CancellationToken ct) where TState : SagaState;

    ValueTask SaveAsync<TState>(Type sagaType, TState state, int expectedVersion,
        CancellationToken ct) where TState : SagaState;

    ValueTask CompleteAsync(Type sagaType, Guid instanceId, CancellationToken ct);
}

internal sealed class EfCoreSagaStore<TDb> : ISagaStore where TDb : DbContext
{
    private readonly TDb _db;
    public EfCoreSagaStore(TDb db) => _db = db;

    public async ValueTask<(TState, int)?> LoadAsync<TState>(Type sagaType, string key, CancellationToken ct)
        where TState : SagaState
    {
        var row = await _db.Set<SagaRow>()
            .Where(r => r.SagaType == sagaType.FullName && r.CorrelationKey == key)
            .FirstOrDefaultAsync(ct);
        if (row is null) return null;

        var state = JsonSerializer.Deserialize<TState>(row.StateJson)!;
        state.Id = row.Id; state.Version = row.Version;
        return (state, row.Version);
    }

    public async ValueTask SaveAsync<TState>(Type sagaType, TState state, int expected, CancellationToken ct)
        where TState : SagaState
    {
        var json = JsonSerializer.Serialize(state);
        if (expected == 0)
        {
            _db.Set<SagaRow>().Add(new SagaRow
            {
                Id = state.Id, SagaType = sagaType.FullName!,
                CorrelationKey = state.Id.ToString(), StateJson = json, Version = 1
            });
        }
        else
        {
            var affected = await _db.Set<SagaRow>()
                .Where(r => r.Id == state.Id && r.Version == expected)
                .ExecuteUpdateAsync(u => u
                    .SetProperty(x => x.StateJson, json)
                    .SetProperty(x => x.Version, expected + 1), ct);
            if (affected == 0)
                throw new SagaConcurrencyException(state.Id, expected);
        }
        await _db.SaveChangesAsync(ct);
    }
}

public sealed class SagaRow
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
}
```

## 3. Saga Middleware — оркестрация вызова

```csharp
public sealed class SagaMiddleware<TSaga, TState> : IBusMiddleware
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly ISagaStore _store;
    private readonly SagaMetadata _meta; // построена генератором из Correlate()

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var correlation = _meta.CorrelationFor(ctx.Message.GetType());
        if (correlation is null) { await next(ctx); return; }

        var key = correlation.Key(ctx.Message);
        var loaded = await _store.LoadAsync<TState>(typeof(TSaga), key, ctx.CancellationToken);

        TSaga saga;
        int expectedVersion;

        if (loaded is null)
        {
            if (!correlation.StartsNew)
            {
                // Нет инстанса и не StartedBy → «поздний хвост»
                await MetricsSagaMissed(ctx);
                return;
            }
            saga = new TSaga { State = new TState { Id = Guid.CreateVersion7(), CreatedAt = DateTime.UtcNow } };
            expectedVersion = 0;
        }
        else
        {
            saga = new TSaga { State = loaded.Value.state };
            expectedVersion = loaded.Value.version;
        }

        saga.Context = new SagaContextImpl(ctx);

        // Диспетчеризация в конкретный Handle(<T>) — через генератор
        await SagaDispatcher<TSaga, TState>.InvokeAsync(saga, ctx.Message);

        // Инварианты
        _meta.CheckInvariants(saga.State);

        await _store.SaveAsync(typeof(TSaga), saga.State, expectedVersion, ctx.CancellationToken);

        if (saga.IsComplete)
            await _store.CompleteAsync(typeof(TSaga), saga.State.Id, ctx.CancellationToken);

        await next(ctx);
    }
}
```

## 4. Стиль B: Durable Execution (Temporal-like)

Идея: сага — обычная функция с `await ctx.WaitFor<T>()`, где каждый шаг детерминирован и checkpointit-ся.

```csharp
public interface ISagaContext
{
    IBus Bus { get; }
    ValueTask<T?> WaitFor<T>(TimeSpan? timeout = null) where T : class;
    ValueTask Send<T>(T cmd) where T : class;
    ValueTask Publish<T>(T evt) where T : class;
    ValueTask<TResult> Step<TResult>(Func<Task<TResult>> action, Func<TResult, Task>? compensate = null);
    ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class;
}

// Реализация — «журнал событий» саги
internal sealed class DurableSagaContext : ISagaContext
{
    private readonly SagaJournal _journal;
    private readonly IBus _bus;
    private int _cursor;

    public DurableSagaContext(SagaJournal journal, IBus bus)
        => (_journal, _bus) = (journal, bus);

    public IBus Bus => _bus;

    // Каждое действие: сначала смотрим в журнал, есть ли уже результат — replay-safe
    public async ValueTask<TResult> Step<TResult>(Func<Task<TResult>> action, Func<TResult, Task>? compensate = null)
    {
        if (_cursor < _journal.Records.Count)
        {
            var replayed = (StepRecord)_journal.Records[_cursor++];
            return JsonSerializer.Deserialize<TResult>(replayed.ResultJson)!;
        }

        var result = await action();
        _journal.Append(new StepRecord(JsonSerializer.Serialize(result)));
        _cursor++;

        if (compensate is not null)
            _journal.RegisterCompensation(() => compensate(result));
        return result;
    }

    public async ValueTask<T?> WaitFor<T>(TimeSpan? timeout = null) where T : class
    {
        if (_cursor < _journal.Records.Count)
        {
            var rec = (WaitRecord)_journal.Records[_cursor++];
            return rec.Payload is null ? null : JsonSerializer.Deserialize<T>(rec.Payload);
        }

        // Регистрируем ожидание — саг-раннер приостанавливается,
        // возобновится при получении сообщения T (по корреляции)
        _journal.Append(new WaitRecord(typeof(T).FullName!, timeout));
        throw new SagaSuspendException();  // ловится раннером
    }

    public ValueTask Send<T>(T cmd) where T : class => _bus.Send(cmd);
    public ValueTask Publish<T>(T evt) where T : class => _bus.Publish(evt);

    public ValueTask RequestTimeoutAsync<T>(T timeoutMsg, TimeSpan delay) where T : class
        => _bus.Schedule(timeoutMsg, DateTimeOffset.UtcNow.Add(delay)).AsValueTask();
}

internal abstract record JournalRecord;
internal sealed record StepRecord(string ResultJson) : JournalRecord;
internal sealed record WaitRecord(string ExpectedType, TimeSpan? Timeout) : JournalRecord;
```

Пример durable-саги:

```csharp
[DurableSaga(CorrelationBy = nameof(OrderPlaced.OrderId))]
public static class BookingSaga
{
    public static async Task Run(OrderPlaced trigger, ISagaContext ctx)
    {
        var hotel = await ctx.Step(
            () => HotelApi.Book(trigger.OrderId),
            compensate: r => HotelApi.Cancel(r.BookingId));

        var flight = await ctx.Step(
            () => FlightApi.Book(trigger.OrderId),
            compensate: r => FlightApi.Refund(r.TicketId));

        await ctx.Send(new RequestPayment(trigger.OrderId, hotel.Amount + flight.Amount));

        var payment = await ctx.WaitFor<PaymentCompleted>(timeout: TimeSpan.FromMinutes(30));
        if (payment is null)
        {
            // Автокомпенсация в обратном порядке через ctx (журнал знает всё)
            await ctx.Publish(new OrderCancelled(trigger.OrderId, "payment-timeout"));
            throw new SagaAbortException();
        }

        await ctx.Publish(new OrderConfirmed(trigger.OrderId));
    }
}
```

## 5. Раннер durable-саги (упрощённый)

```csharp
internal sealed class DurableSagaRunner
{
    private readonly ISagaJournalStore _store;
    private readonly IBus _bus;

    public async Task<SagaOutcome> DispatchAsync(Type sagaType, object message, string correlationKey)
    {
        var journal = await _store.LoadOrCreateAsync(sagaType, correlationKey);
        journal.AppendIncoming(message);

        var ctx = new DurableSagaContext(journal, _bus);
        var entrypoint = SagaCatalog.EntrypointFor(sagaType, message.GetType());

        try
        {
            await entrypoint(message, ctx);
            journal.MarkComplete();
        }
        catch (SagaSuspendException)
        {
            // ждём следующего сообщения
        }
        catch (SagaAbortException)
        {
            await journal.RunCompensationsAsync();
            journal.MarkAborted();
        }

        await _store.SaveAsync(journal);
        return journal.Outcome;
    }
}
```

## 6. Timeouts (durable, переживают рестарт)

Таймауты — обычные сообщения, отправленные через `bus.Schedule` (см. `docs/14-implementation-core.md`, §6): при срабатывании они приходят в middleware саги и возобновляют её так же, как любое другое сообщение.

## 7. Тест-харнесс саги

```csharp
public sealed class SagaScenario<TSaga, TState> where TSaga : Saga<TState>, new() where TState : SagaState, new()
{
    private readonly List<object> _givens = new();
    private readonly List<object> _whens = new();
    private readonly List<Action<TState>> _stateAssertions = new();
    private readonly List<(Type type, Delegate check)> _sentAssertions = new();

    public static SagaScenario<TSaga, TState> Start() => new();

    public SagaScenario<TSaga, TState> Given<T>(T msg) where T : class { _givens.Add(msg); return this; }
    public SagaScenario<TSaga, TState> When<T>(T msg) where T : class  { _whens.Add(msg);  return this; }

    public SagaScenario<TSaga, TState> ThenSent<T>(Func<T, bool> pred) where T : class
    { _sentAssertions.Add((typeof(T), pred)); return this; }

    public SagaScenario<TSaga, TState> ThenState(Action<TState> assert)
    { _stateAssertions.Add(assert); return this; }

    public async Task RunAsync()
    {
        await using var h = AvtoBusTestHarness.Create(s => s.AddSaga<TSaga, TState>());
        foreach (var g in _givens) await h.Deliver(g);
        h.CapturedSent.Clear();
        foreach (var w in _whens) await h.Deliver(w);
        var state = await h.LoadSaga<TSaga, TState>();

        foreach (var a in _stateAssertions) a(state);
        foreach (var (t, pred) in _sentAssertions)
        {
            var sent = h.CapturedSent.Where(x => x.GetType() == t);
            if (!sent.Any(m => (bool)pred.DynamicInvoke(m)!))
                throw new Xunit.Sdk.XunitException($"Expected sent {t.Name} matching predicate");
        }
    }
}
```

Пример теста:

```csharp
[Fact]
public Task OrderSaga_pays_and_ships() =>
    SagaScenario<OrderSaga, OrderSagaState>.Start()
        .Given(new OrderPlaced(Guid.NewGuid(), Total: 100m))
        .When(new PaymentCompleted(orderId: /*same*/ ...))
        .ThenSent<CreateShipment>(c => c.OrderId != Guid.Empty)
        .ThenState(s => s.Paid && s.Status == "Shipping")
        .RunAsync();
```

## 8. SLA-мониторы (идея 221)

```csharp
[SagaSla(from: typeof(OrderPlaced), to: typeof(OrderFulfilled), maxDuration: "02:00:00")]
public sealed class OrderSaga : Saga<OrderSagaState> { ... }

// Background job раз в минуту:
// - находит инстансы, где OrderPlaced был >2ч назад и нет OrderFulfilled
// - публикует SagaSlaViolated(orderId, elapsed) — на это можно алертить
```
