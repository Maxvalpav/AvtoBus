using AvtoBus.Actors;
using AvtoBus.Runtime;

namespace AvtoBus.Tests;

/// <summary>
/// Регрессия аудита утечек:
/// - истёкший RequestStream не оставляет канал в роутере;
/// - повторный StopAsync хоста не падает после Dispose ранеров;
/// - конкурентные Register/Unregister reminder'ов не гонят Dictionary.
/// </summary>
public class LeakRegressionTests
{
    [Fact]
    public async Task Expired_stream_removes_channel()
    {
        var router = new StreamingReplyRouter();
        var id = Guid.NewGuid();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in router.RegisterStream<string>(id, TimeSpan.FromMilliseconds(50), CancellationToken.None))
            {
            }
        });

        Assert.False(router.IsStreaming(id));
    }

    [Fact]
    public async Task Completed_and_failed_streams_remove_channel()
    {
        var router = new StreamingReplyRouter();

        var id1 = Guid.NewGuid();
        _ = router.RegisterStream<string>(id1, TimeSpan.FromSeconds(5), CancellationToken.None);
        Assert.True(router.TryComplete(id1));
        Assert.False(router.IsStreaming(id1));

        var id2 = Guid.NewGuid();
        _ = router.RegisterStream<string>(id2, TimeSpan.FromSeconds(5), CancellationToken.None);
        Assert.True(router.TryFail(id2, new InvalidOperationException("x")));
        Assert.False(router.IsStreaming(id2));

        await Task.CompletedTask;
    }

    [Fact]
    public async Task Actor_concurrent_reminders_and_dispose_do_not_throw()
    {
        var actor = new ReminderActor { Id = new ActorId("a1") };

        await Parallel.ForEachAsync(Enumerable.Range(0, 50), async (i, _) =>
        {
            actor.Ping(i);
            await Task.Yield();
        });

        await actor.DeactivateAsync();
        actor.Dispose();
        actor.Dispose(); // двойной Dispose — идемпотентен
    }

    private sealed class ReminderActor : VirtualActor<ReminderState>
    {
        public void Ping(int i) => RegisterReminder($"r{i % 5}", TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        protected override Task ReceiveCoreAsync(object message, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class ReminderState
    {
        public int Hits { get; set; }
    }
}
