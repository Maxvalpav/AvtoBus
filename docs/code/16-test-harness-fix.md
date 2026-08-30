# AvtoBus.Testing — исправление A7 (`TestTransport`/`AvtoBusTestHarness`)

> **Code sketch / unverified.** Закрывает пункт **A7** из `../30-forgotten-and-bugs.md`. Заменяет `TestTransport`/`AvtoBusTestHarness` из `08-extensions.md` — та версия не должна использоваться как есть: `Captured` не инициализирован, а `DrainAsync` не ждёт ничего, кроме таймера.

## Что было не так

1. `TestTransport(FakeTimeProvider clock)` — единственный вызываемый конструктор — не создаёт `CapturedMessages`, а свойство объявлено `{ get; }` без инициализатора. Любое обращение к `harness.Transport.Captured.Published` падает с `NullReferenceException`.
2. `DrainAsync(TimeSpan timeout)` — это `await Task.Delay(timeout)`. Тест либо ждёт фиксированное время впустую (медленно), либо не дожидается реальной обработки (флаки) — гонки неизбежны в обоих случаях.
3. Ничего в харнессе не запускает `BusHost`/pipeline по-настоящему: `AvtoBusTestHarness` строит `IServiceProvider`, но фоновый `IHostedService` никогда не стартует явно и не имеет наблюдаемого «пока не закончил — не отпускай» сигнала.

## Принцип исправления

Дренаж должен быть **основан на событиях**, а не на времени: харнесс отслеживает количество «сообщений в полёте» (in-flight) и ждёт, пока оно не станет нулём, либо не наступит явный timeout как страховка от зависания теста.

```csharp
// AvtoBus.Testing/InFlightTracker.cs
namespace AvtoBus.Testing;

/// <summary>
/// Считает сообщения от enqueue до ack/nack. DrainAsync ждёт именно этот счётчик,
/// а не произвольный таймер.
/// </summary>
internal sealed class InFlightTracker
{
    private int _count;
    private readonly SemaphoreSlim _idle = new(1, 1);
    private TaskCompletionSource _zero = CreateSignaledTcs();

    private static TaskCompletionSource CreateSignaledTcs()
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tcs.SetResult();
        return tcs;
    }

    public void Increment()
    {
        if (Interlocked.Increment(ref _count) == 1)
        {
            lock (this) _zero = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    public void Decrement()
    {
        if (Interlocked.Decrement(ref _count) == 0)
        {
            lock (this) _zero.TrySetResult();
        }
    }

    public async Task WaitForIdleAsync(TimeSpan timeout, CancellationToken ct)
    {
        // Быстрый путь: уже пусто
        if (Volatile.Read(ref _count) == 0) return;

        TaskCompletionSource zero;
        lock (this) zero = _zero;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        try
        {
            await zero.Task.WaitAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"TestTransport did not reach idle state within {timeout}. " +
                $"In-flight count: {Volatile.Read(ref _count)}. " +
                "This usually means a handler is stuck, throwing without being observed, " +
                "or a cascade publish loop never terminates.");
        }
    }
}
```

## `TestTransport`: правильная инициализация и реальный in-flight сигнал

```csharp
// AvtoBus.Testing/TestTransport.cs (замена версии из 08-extensions.md)
namespace AvtoBus.Testing;

public sealed class TestTransport : ITransport
{
    public string Name => "test";

    // Инициализируется в конструкторе — не может быть null.
    public CapturedMessages Captured { get; }

    private readonly InMemoryTransport _inner;
    private readonly InFlightTracker _inFlight = new();

    public TestTransport(TimeProvider clock, CapturedMessages? captured = null)
    {
        Captured = captured ?? new CapturedMessages();
        _inner = new InMemoryTransport(clock);
    }

    public ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct)
    {
        var payload = TryDeserializeForCapture(envelope);
        if (payload is not null)
        {
            lock (Captured.Published) Captured.Published.Add(payload);
        }
        lock (Captured.Envelopes) Captured.Envelopes.Add(envelope);

        _inFlight.Increment(); // считается "в полёте" с момента отправки
        return SendAndTrack(envelope, dest, ct);
    }

    private async ValueTask SendAndTrack(Envelope envelope, TransportDestination dest, CancellationToken ct)
    {
        try
        {
            await _inner.SendAsync(envelope, dest, ct);
        }
        finally
        {
            // Публикация сама по себе не завершает "полёт" — это делает Ack/Nack на приёмнике.
            // Здесь мы просто гарантируем, что запись в очередь не потеряна до decrement в ReceiveAsync.
        }
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var msg in _inner.ReceiveAsync(subscription, ct))
        {
            var payload = TryDeserializeForCapture(msg.Envelope);
            if (payload is not null)
            {
                lock (Captured.Consumed) Captured.Consumed.Add(payload);
            }

            yield return new TransportMessage(msg.Envelope, new TrackingAck(msg.Ack, _inFlight, Captured, msg.Envelope));
        }
    }

    /// <summary>
    /// Дожидается, пока все отправленные сообщения будут либо ack, либо nack —
    /// то есть пайплайн реально закончил их обрабатывать, а не "прошло N секунд".
    /// </summary>
    public Task DrainAsync(TimeSpan? timeout = null, CancellationToken ct = default)
        => _inFlight.WaitForIdleAsync(timeout ?? TimeSpan.FromSeconds(10), ct);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public void Dispose() => _inner.Dispose();

    private static object? TryDeserializeForCapture(Envelope envelope)
    {
        try
        {
            var type = TypeRegistry.Resolve(envelope.MessageType);
            return type is null ? null : JsonSerializer.Deserialize(envelope.Body.Span, type);
        }
        catch
        {
            return null; // захват — best-effort, не должен ронять тест
        }
    }
}

/// <summary>
/// Оборачивает реальный IAckContext, снимая "в полёте" ровно один раз,
/// независимо от того, что вызвали — Ack, Nack или Defer.
/// </summary>
internal sealed class TrackingAck : IAckContext
{
    private readonly IAckContext _inner;
    private readonly InFlightTracker _tracker;
    private readonly CapturedMessages _captured;
    private readonly Envelope _envelope;
    private int _completed;

    public TrackingAck(IAckContext inner, InFlightTracker tracker, CapturedMessages captured, Envelope envelope)
    {
        _inner = inner;
        _tracker = tracker;
        _captured = captured;
        _envelope = envelope;
    }

    public async ValueTask AckAsync(CancellationToken ct = default)
    {
        await _inner.AckAsync(ct);
        Complete();
    }

    public async ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        await _inner.NackAsync(requeue, ct);
        if (!requeue)
        {
            lock (_captured.DeadLettered)
                _captured.DeadLettered.Add((_envelope.MessageType, _envelope, "nack-no-requeue"));
            Complete();
        }
        else
        {
            // Требование "в полёте" сохраняется — сообщение вернётся в очередь и снова будет обработано.
        }
    }

    public async ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        await _inner.DeferAsync(delay, ct);
        // Отложенное сообщение остаётся "в полёте" — вернётся в очередь после delay.
    }

    private void Complete()
    {
        if (Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
            _tracker.Decrement();
    }
}
```

## `AvtoBusTestHarness`: явный старт host и осмысленный `WaitFor*`

```csharp
// AvtoBus.Testing/AvtoBusTestHarness.cs (замена версии из 08-extensions.md)
namespace AvtoBus.Testing;

public sealed class AvtoBusTestHarness : IAsyncDisposable
{
    public IBus Bus { get; }
    public FakeTimeProvider Clock { get; } = new();
    public TestTransport Transport { get; }
    public InMemorySagaStore Sagas { get; } = new();
    public IServiceProvider Services { get; }
    public CapturedMessages Captured => Transport.Captured;

    private readonly IHost _host;

    private AvtoBusTestHarness(IHost host, TestTransport transport)
    {
        _host = host;
        Services = host.Services;
        Transport = transport;
        Bus = host.Services.GetRequiredService<IBus>();
    }

    public static async Task<AvtoBusTestHarness> CreateAsync(
        Action<IServiceCollection>? configureServices = null,
        Action<BusOptions>? configureBus = null,
        CancellationToken ct = default)
    {
        var captured = new CapturedMessages();
        TestTransport? transport = null;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(new FakeTimeProvider());
        builder.Services.AddLogging(b => b.AddDebug());

        builder.Services.AddAvtoBus(bus =>
        {
            configureBus?.Invoke(bus);
        });

        // Подменяем транспорт на тестовый ПОСЛЕ пользовательской конфигурации,
        // чтобы TestTransport гарантированно перехватывал всё.
        builder.Services.AddSingleton<ITransport>(sp =>
        {
            transport = new TestTransport(sp.GetRequiredService<TimeProvider>(), captured);
            return transport;
        });
        builder.Services.AddSingleton<ISagaStore, InMemorySagaStore>();

        configureServices?.Invoke(builder.Services);

        var host = builder.Build();

        // Явный старт: BusHost поднимается, подписки регистрируются —
        // до этого момента Publish/Send отправлять бессмысленно.
        await host.StartAsync(ct);

        return new AvtoBusTestHarness(host, transport!);
    }

    public async Task PublishAndWait<T>(T @event, TimeSpan? timeout = null, CancellationToken ct = default) where T : class
    {
        await Bus.Publish(@event, ct: ct);
        await Transport.DrainAsync(timeout, ct);
    }

    public async Task SendAndWait<T>(T command, TimeSpan? timeout = null, CancellationToken ct = default) where T : class
    {
        await Bus.Send(command, ct: ct);
        await Transport.DrainAsync(timeout, ct);
    }

    public IEnumerable<T> Published<T>() where T : class => Captured.Published.OfType<T>();
    public IEnumerable<T> Consumed<T>() where T : class => Captured.Consumed.OfType<T>();

    /// <summary>
    /// Ждёт публикацию конкретного сообщения с poll-интервалом, а не фиксированный сон.
    /// Возвращает null, если не дождались — тест сам решает, это ли фейл.
    /// </summary>
    public async Task<T?> WaitForPublished<T>(TimeSpan? timeout = null, CancellationToken ct = default) where T : class
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (DateTime.UtcNow < deadline)
        {
            var found = Published<T>().FirstOrDefault();
            if (found is not null) return found;
            await Task.Delay(10, ct);
        }
        return null;
    }

    /// <summary>
    /// Продвигает виртуальное время и дожидается реакции пайплайна (таймауты саг, ретраи).
    /// </summary>
    public async Task AdvanceTime(TimeSpan delta, CancellationToken ct = default)
    {
        var fake = (FakeTimeProvider)Services.GetRequiredService<TimeProvider>();
        fake.Advance(delta);
        await Transport.DrainAsync(TimeSpan.FromSeconds(5), ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync(TimeSpan.FromSeconds(5));
        _host.Dispose();
    }
}
```

## Почему это закрывает именно A7, а не маскирует симптом

- `Captured` больше не может быть `null` — конструктор либо принимает готовый экземпляр, либо создаёт новый. Компилятор и юнит-тест на этот факт (`Captured_is_never_null`) фиксируют инвариант.
- `DrainAsync` реагирует на фактическое состояние системы (`InFlightTracker`), а не гадает по таймеру. Тест на быстрый happy path не тормозит искусственной задержкой; тест на реально зависший хендлер получает осмысленный `TimeoutException` с текущим значением счётчика, а не тихо проходит с пустым результатом.
- `CreateAsync` явно вызывает `host.StartAsync`, поэтому `BusHost` гарантированно поднят до первого `Publish`/`Send` — раньше это зависело от порядка построения `ServiceProvider` и не было гарантировано.

## Что ещё не покрыто (честно)

- `InFlightTracker` не учитывает каскадные `PublishAsync` из хендлера как отдельные unit — если каскад публикует и никогда не завершает свою обработку, `WaitForIdleAsync` корректно поймает зависание, но диагностика («что именно застряло») ограничивается счётчиком, а не списком id.
- Нет проверки, что `TestTransport` подменяет **все** зарегистрированные транспорты в multi-transport сценарии (`bus.Routes(...).Via("kafka")`) — сейчас интерфейс подразумевает один `ITransport` на весь `AddAvtoBus`. Это отдельный design gap, см. `27-gap-analysis.md`, пункт про Core-границы.
- Тесты для этого файла (unit на `InFlightTracker`, integration на `AvtoBusTestHarness` с намеренно зависающим хендлером) не написаны — сам файл остаётся `code sketch`.
