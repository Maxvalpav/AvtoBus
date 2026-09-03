using AvtoBus.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Sagas;

/// <summary>
/// Декларативный тестовый сценарий саги (док 17, §7): Given —&gt; When —&gt; ThenSent/ThenState.
/// Поднимает шину in-memory, прогоняет сообщения и проверяет каскады и итоговое состояние.
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
    "Сценарий регистрирует сагу через рефлексию и диспатчит dynamic — тестовый DSL, несовместим с trimming/AOT.")]
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
    "Сценарий диспатчит сообщения через dynamic — тестовый DSL, несовместим с NativeAOT.")]
public sealed class SagaScenario<TSaga, TState>
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly List<object> _givens = [];
    private readonly List<object> _whens = [];
    private readonly List<(Type Type, Delegate Predicate)> _sent = [];
    private readonly List<(Func<TState, bool> Predicate, string Name)> _state = [];
    private TimeSpan _timeout = TimeSpan.FromSeconds(5);

    public static SagaScenario<TSaga, TState> Start() => new();

    /// <summary>Контекст, из которого инстанс уже существует (обычно первое сообщение — StartsNew).</summary>
    public SagaScenario<TSaga, TState> Given<T>(T message) where T : class
    {
        _givens.Add(message);
        return this;
    }

    /// <summary>Сообщение, которое продвигает сагу.</summary>
    public SagaScenario<TSaga, TState> When<T>(T message) where T : class
    {
        _whens.Add(message);
        return this;
    }

    /// <summary>Утверждение: сага опубликовала каскадное сообщение, удовлетворяющее предикату.</summary>
    public SagaScenario<TSaga, TState> ThenSent<T>(Func<T, bool> predicate) where T : class
    {
        _sent.Add((typeof(T), predicate));
        return this;
    }

    /// <summary>Утверждение по итоговому состоянию саги.</summary>
    public SagaScenario<TSaga, TState> ThenState(Func<TState, bool> predicate, string name = "состояние")
    {
        _state.Add((predicate, name));
        return this;
    }

    public SagaScenario<TSaga, TState> WithTimeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    public async Task RunAsync()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddSaga<TSaga, TState>());

        var store = harness.Services.GetRequiredService<ISagaStore>();
        var key = CorrelationKey();

        // Given: сага стартует (StartsNew) — дожидаемся появления состояния в хранилище.
        foreach (var given in _givens)
        {
            await DeliverAsync(harness, given);
            await WaitAsync(
                async () => await CurrentVersionAsync(store, key) is not null);
        }

        // When: состояние уже существует — дожидаемся увеличения версии (шаг выполнен).
        foreach (var when in _whens)
        {
            var before = await CurrentVersionAsync(store, key);
            await DeliverAsync(harness, when);
            await WaitAsync(
                async () => await CurrentVersionAsync(store, key) is int v && v > (before ?? 0));
        }

        // Каскады, отправленные сагой. Запись каскада рекордером происходит после
        // завершения обработки сообщения, поэтому ждём появления, а не проверяем мгновенно.
        foreach (var (type, predicate) in _sent)
        {
            await WaitAsync(
                () => Task.FromResult(harness.Recorder.Published
                    .Where(m => m.Message?.GetType() == type)
                    .Select(m => m.Message!)
                    .Any(m => (bool)predicate.DynamicInvoke(m)!)));
        }

        // Итоговое состояние.
        var loaded = await store.LoadAsync<TState>(typeof(TSaga), key, CancellationToken.None);
        if (loaded is null)
            throw new SagaScenarioAssertionException(
                $"Сага {typeof(TSaga).Name} не найдена по ключу корреляции '{key}'.");

        foreach (var (predicate, name) in _state)
        {
            if (!predicate(loaded.Value.state))
                throw new SagaScenarioAssertionException($"Инвариант '{name}' не выполнен.");
        }
    }

    private async Task<int?> CurrentVersionAsync(ISagaStore store, string key)
    {
        var loaded = await store.LoadAsync<TState>(typeof(TSaga), key, CancellationToken.None);
        return loaded is null ? null : loaded.Value.version;
    }

    /// <summary>Асинхронный поллинг условия с таймаутом.</summary>
    private async Task WaitAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + _timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;

            await Task.Delay(25);
        }

        throw new TimeoutException($"Сценарий не завершился за {_timeout}.");
    }

    private string CorrelationKey()
    {
        var meta = SagaMetadata<TSaga, TState>.Build();

        // Ключ берём из первого сообщения сценария — оно стартует инстанс.
        var first = _givens.Count > 0 ? _givens[0] : _whens[0];
        var correlation = meta.CorrelationFor(first)
                          ?? throw new SagaScenarioAssertionException(
                              $"{first.GetType().Name} не коррелируется с сагой {typeof(TSaga).Name}.");

        return correlation.Key(first);
    }

    private static async Task DeliverAsync(AvtoBusTestHarness harness, object message)
    {
        // Отправляем по runtime-типу наследника/интерфейса: routing смотрит на конкретный тип,
        // а не на object.
        var dynamicMessage = (dynamic)message;
        if (message is ICommand command)
            await harness.Bus.SendAsync(command);
        else
            await harness.Bus.PublishAsync(dynamicMessage);
    }
}

/// <summary>Провал утверждения сценария саги.</summary>
public sealed class SagaScenarioAssertionException(string message) : Exception(message);
