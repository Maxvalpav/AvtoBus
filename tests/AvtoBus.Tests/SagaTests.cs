using AvtoBus.Sagas;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public sealed class OrderSagaState : SagaState
{
    public Guid OrderId { get; set; }
    public decimal Total { get; set; }
    public bool Paid { get; set; }
    public bool Shipped { get; set; }
}

public sealed class OrderSaga : Saga<OrderSagaState>, IStartedBy<OrderPlaced>, IHandle<OrderPaid>
{
    protected override void Correlate(SagaMap<OrderSagaState> map)
    {
        map.On<OrderPlaced>(m => m.OrderId).StartsNew();
        map.On<OrderPaid>(m => m.OrderId);
    }

    protected override void Invariants(SagaInvariants<OrderSagaState> inv)
        => inv.Assert(s => s.Total > 0, "total-must-be-positive");

    public Task Handle(OrderPlaced message)
    {
        State.OrderId = message.OrderId;
        State.Total = message.Total;
        State.Status = "Placed";
        return Task.CompletedTask;
    }

    public Task Handle(OrderPaid message)
    {
        State.Paid = true;
        State.Status = "Paid";
        return Publish(new ShipmentCreated(State.OrderId)).AsTask();
    }
}

public class SagaScenarioTests
{
    [Fact]
    public async Task OrderSaga_full_scenario()
    {
        var orderId = Guid.NewGuid();

        await SagaScenario<OrderSaga, OrderSagaState>.Start()
            .Given(new OrderPlaced(orderId, 100m))
            .When(new OrderPaid(orderId))
            .ThenSent<ShipmentCreated>(m => m.OrderId == orderId)
            .ThenState(s => s.Paid && s.Status == "Paid")
            .RunAsync();
    }

    [Fact]
    public async Task OrderSaga_skips_message_for_unknown_instance()
    {
        var orderId = Guid.NewGuid();

        // OrderPaid (не StartsNew) при отсутствующем инстансе — ShouldSkip, не ошибка.
        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(
            bus => bus.AddSaga<OrderSaga, OrderSagaState>());

        await harness.Bus.PublishAsync(new OrderPaid(orderId));
        await harness.WaitUntilAsync(
            () => harness.Recorder.Consumed.Any(c => c.Message is OrderPaid));

        var store = harness.Services.GetRequiredService<ISagaStore>();
        var state = await store.LoadAsync<OrderSagaState>(typeof(OrderSaga), orderId.ToString());

        Assert.Null(state);
    }

    [Fact]
    public async Task OrderSaga_invariant_violation_surfaces_as_error()
    {
        var orderId = Guid.NewGuid();

        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(
            bus => bus.AddSaga<OrderSaga, OrderSagaState>());

        // Отправляем событие, нарушающее инвариант: состояние помечается "Paid" без Total.
        await harness.Bus.PublishAsync(new OrderPlaced(orderId, 0m));
        await harness.WaitUntilAsync(
            () => harness.Recorder.Consumed.Any(c => c.Message is OrderPlaced));

        var store = harness.Services.GetRequiredService<ISagaStore>();
        var state = await store.LoadAsync<OrderSagaState>(typeof(OrderSaga), orderId.ToString());

        // Инвариант не выполнен — состояние в хранилище не сохранено (упало до save).
        Assert.Null(state);
    }

    [Fact]
    public async Task SagaState_version_increments_on_each_step()
    {
        var orderId = Guid.NewGuid();
        var key = orderId.ToString();

        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(
            bus => bus.AddSaga<OrderSaga, OrderSagaState>());

        var store = harness.Services.GetRequiredService<ISagaStore>();

        await harness.Bus.PublishAsync(new OrderPlaced(orderId, 100m));
        await harness.WaitUntilAsync(
            () => LoadVersion(store, key) is not null);

        Assert.Equal(1, LoadVersion(store, key));

        await harness.Bus.PublishAsync(new OrderPaid(orderId));
        await harness.WaitUntilAsync(
            () => LoadVersion(store, key) is int v && v == 2);

        Assert.Equal(2, LoadVersion(store, key));
    }

    private static int? LoadVersion(ISagaStore store, string key)
    {
        var loaded = store.LoadAsync<OrderSagaState>(typeof(OrderSaga), key).AsTask().GetAwaiter().GetResult();
        return loaded is null ? null : loaded.Value.version;
    }
}

// ===== Durable execution (стиль B) =====

[DurableSaga(CorrelationBy = nameof(OrderPlaced.OrderId))]
public static class BookingDurableSaga
{
    private sealed record BookingResult(string BookingId, decimal Price);

    public static async Task Run(OrderPlaced trigger, ISagaContext ctx)
    {
        var hotel = await ctx.Step(
            () => Task.FromResult(new BookingResult("H-" + trigger.OrderId, 80m)));

        await ctx.Send(new ChargeCard(trigger.OrderId, hotel.Price));

        var payment = await ctx.WaitFor<OrderPaid>(TimeSpan.FromMinutes(30));
        if (payment is null)
            throw new SagaAbortException("payment-timeout");

        await ctx.Publish(new ShipmentCreated(trigger.OrderId));
    }
}

public class DurableSagaTests
{
    [Fact]
    public async Task DurableRunner_suspends_then_resumes()
    {
        var orderId = Guid.NewGuid();
        var store = new InMemorySagaJournalStore();
        var bus = new RecordingBus();
        var runner = new DurableSagaRunner(store, bus);

        var first = await runner.DispatchAsync(typeof(BookingDurableSaga), new OrderPlaced(orderId, 100m), orderId.ToString());
        Assert.Equal(SagaOutcome.Suspended, first);

        var journal = await store.LoadOrCreateAsync(typeof(BookingDurableSaga), orderId.ToString(), null, CancellationToken.None);
        Assert.Equal(2, journal.Records.Count);

        var second = await runner.DispatchAsync(typeof(BookingDurableSaga), new OrderPaid(orderId), orderId.ToString());
        Assert.Equal(SagaOutcome.Completed, second);
        Assert.Contains(bus.Published, m => m is ShipmentCreated { OrderId: var o } && o == orderId);
    }

    [Fact]
    public async Task BookingSaga_runs_happy_path()
    {
        var orderId = Guid.NewGuid();

        // Слушаем ShipmentCreated — его публикует сага после WaitFor.
        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddDurableSaga(typeof(BookingDurableSaga),
                    typeof(OrderPlaced), typeof(OrderPaid))
                .Subscribe<ShipmentCreated>(_ => Task.CompletedTask));

        await harness.Bus.PublishAsync(new OrderPlaced(orderId, 100m));

        // Сага приостановлена на WaitFor<OrderPaid>: ShipmentCreated ещё нет.
        await Task.Delay(150);
        Assert.Empty(harness.Recorder.ConsumedOf<ShipmentCreated>());

        await harness.Bus.PublishAsync(new OrderPaid(orderId));
        await harness.WaitUntilAsync(() => harness.Recorder.ConsumedOf<ShipmentCreated>().Any());

        Assert.Single(harness.Recorder.ConsumedOf<ShipmentCreated>(),
            s => s.OrderId == orderId);
    }

    [Fact]
    public async Task BookingSaga_replays_steps_on_resume()
    {
        var orderId = Guid.NewGuid();

        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddDurableSaga(typeof(BookingDurableSaga),
                    typeof(OrderPlaced), typeof(OrderPaid))
                .Subscribe<ShipmentCreated>(_ => Task.CompletedTask));

        var journalStore = harness.Services.GetRequiredService<ISagaJournalStore>();
        var key = orderId.ToString();

        await harness.Bus.PublishAsync(new OrderPlaced(orderId, 100m));
        await Task.Delay(150);

        // После триггера журнал содержит Step + Wait (Suspended).
        var journal = await journalStore.LoadOrCreateAsync(typeof(BookingDurableSaga), key, null, CancellationToken.None);
        Assert.Equal(2, journal.Records.Count);
        Assert.Equal(typeof(StepRecord), journal.Records[0].GetType());
        Assert.Equal(typeof(WaitRecord), journal.Records[1].GetType());

        // Резюмируем OrderPaid: шаг НЕ должен перевыполниться (result из журнала), сага завершается.
        await harness.Bus.PublishAsync(new OrderPaid(orderId));
        await harness.WaitUntilAsync(() => harness.Recorder.ConsumedOf<ShipmentCreated>().Any());

        var after = await journalStore.LoadOrCreateAsync(typeof(BookingDurableSaga), key, null, CancellationToken.None);
        Assert.Equal(SagaOutcome.Completed, after.Outcome);
        Assert.Single(after.Records.OfType<StepRecord>());
    }

    /// <summary>Fake IBus для прямых тестов durable-раннера без поднятия шины.</summary>
    private sealed class RecordingBus : IBus
    {
        public List<object> Published { get; } = [];
        public List<object> Sent { get; } = [];

        public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default) where T : class
        {
            Published.Add(@event!);
            return ValueTask.CompletedTask;
        }

        public ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default) where T : class
        {
            Sent.Add(command!);
            return ValueTask.CompletedTask;
        }

        public ValueTask<TReply> RequestAsync<TRequest, TReply>(TRequest request, TimeSpan? timeout = null, CancellationToken ct = default)
            where TRequest : class
            where TReply : class
            => throw new NotImplementedException();

        public ValueTask<ScheduledToken> ScheduleAsync<T>(T message, DateTimeOffset at, CancellationToken ct = default) where T : class
            => throw new NotImplementedException();

        public ValueTask CancelScheduledAsync(ScheduledToken token, CancellationToken ct = default)
            => throw new NotImplementedException();

        public ValueTask EnqueueLocal<T>(T message, string? queueName = null, CancellationToken ct = default)
            where T : class
            => throw new NotImplementedException();
    }
}
