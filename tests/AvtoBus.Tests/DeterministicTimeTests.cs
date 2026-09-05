using AvtoBus.Actors;
using AvtoBus.Sagas;
using AvtoBus.Scheduling;
using MsFakeTimeProvider = Microsoft.Extensions.Time.Testing.FakeTimeProvider;

namespace AvtoBus.Tests;

/// <summary>
/// Детерминированные тесты времени (аудит 03 §2.3): FakeTimeProvider вместо
/// реальных ожиданий — без задержек и флаков.
/// </summary>
public class DeterministicTimeTests
{
    [Fact]
    public async Task Actor_reminder_fires_on_time_advance()
    {
        // Внимание: в AvtoBus.Tests есть свой локальный FakeTimeProvider (только GetUtcNow,
        // без таймеров). Здесь нужен именно Microsoft.Extensions.Time.Testing.FakeTimeProvider,
        // иначе Task.Delay(..., time) не проснётся от Advance.
        var time = new MsFakeTimeProvider(new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero));
        var actor = new ClockActor { Id = new ActorId("clock-1"), Clock = time };
        actor.FireIn(TimeSpan.FromHours(1));

        // Даём fire-and-forget циклу дойти до Task.Delay и создать таймер на фейковых часах,
        // иначе Advance случится раньше подписки и никого не разбудит.
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        Assert.False(actor.Fired.Task.IsCompleted);
        var before = time.GetUtcNow();
        time.Advance(TimeSpan.FromMinutes(61));
        var after = time.GetUtcNow();
        await Task.WhenAny(actor.Fired.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.True(actor.Fired.Task.IsCompleted, $"Reminder did not fire before={before} after={after}.");
        Assert.True(await actor.Fired.Task);
        await actor.DeactivateAsync();
    }

    [Fact]
    public async Task Schedule_store_claims_only_due_by_fake_time()
    {
        var start = new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var time = new MsFakeTimeProvider(start);
        var store = new InMemoryScheduleStore(time);

        var token = Guid.NewGuid();
        await store.ScheduleAsync(new ScheduledMessage
        {
            Token = token,
            MessageType = "t",
            Destination = "q",
            Transport = "inmemory",
            DeliverAt = start.AddHours(1).UtcDateTime,
            CreatedAt = start.UtcDateTime,
        });

        var early = await store.ClaimDueAsync(start.UtcDateTime, 10, "test");
        Assert.Empty(early);

        time.Advance(TimeSpan.FromHours(2));
        var due = await store.ClaimDueAsync(time.GetUtcNow().UtcDateTime, 10, "test");
        Assert.Single(due);
    }

    [Fact]
    public async Task Saga_scenario_runs_with_fake_time()
    {
        var time = new MsFakeTimeProvider(new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero));
        var orderId = Guid.NewGuid();

        // Сага завершается реальной обработкой (быстро); дедлайн ожидания — по фейковым часам.
        await SagaScenario<OrderSaga, OrderSagaState>.Start()
            .WithTime(time)
            .Given(new AvtoBus.Tests.Contracts.OrderPlaced(orderId, 100m))
            .When(new AvtoBus.Tests.Contracts.OrderPaid(orderId))
            .ThenSent<AvtoBus.Tests.Contracts.ShipmentCreated>(m => m.OrderId == orderId)
            .RunAsync();
    }

    private sealed class ClockState
    {
        public int Hits { get; set; }
    }

    private sealed class ClockActor : VirtualActor<ClockState>
    {
        public new TimeProvider Clock
        {
            get => base.Clock;
            set => base.Clock = value;
        }

        public TaskCompletionSource<bool> Fired { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void FireIn(TimeSpan due) => RegisterReminder("clock", due, TimeSpan.FromHours(1));

        public override Task OnReminderAsync(string name, CancellationToken ct)
        {
            Fired.TrySetResult(true);
            return Task.CompletedTask;
        }

        protected override Task ReceiveCoreAsync(object message, CancellationToken ct) => Task.CompletedTask;
    }
}
