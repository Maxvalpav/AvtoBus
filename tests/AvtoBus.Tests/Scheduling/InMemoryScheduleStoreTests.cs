using AvtoBus.Scheduling;
using Xunit;

namespace AvtoBus.Tests.Scheduling;

public class InMemoryScheduleStoreTests
{
    [Fact]
    public async Task Schedule_and_claim_due_then_mark_delivered()
    {
        var store = new InMemoryScheduleStore();
        var token = await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            DeliverAt = DateTime.UtcNow.AddMinutes(-1),
            Destination = "inmem",
        });

        var due = await store.ClaimDueAsync(DateTime.UtcNow, 10, "test");
        Assert.Single(due);
        Assert.Equal(token, due[0].Token);

        await store.MarkDeliveredAsync(new[] { due[0].Id });
        var again = await store.ClaimDueAsync(DateTime.UtcNow, 10, "test");
        Assert.Empty(again);
    }

    [Fact]
    public async Task Not_due_messages_are_not_claimed()
    {
        var store = new InMemoryScheduleStore();
        await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            DeliverAt = DateTime.UtcNow.AddHours(2),
            Destination = "inmem",
        });

        var due = await store.ClaimDueAsync(DateTime.UtcNow, 10, "test");
        Assert.Empty(due);
    }

    [Fact]
    public async Task Unique_key_deduplicates_pending_schedule()
    {
        var store = new InMemoryScheduleStore();
        var t1 = await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            UniqueKey = "job-1",
            DeliverAt = DateTime.UtcNow.AddMinutes(1),
            Destination = "q",
        });
        var t2 = await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            UniqueKey = "job-1",
            DeliverAt = DateTime.UtcNow.AddMinutes(1),
            Destination = "q",
        });

        Assert.Equal(t1, t2);
        var due = await store.ClaimDueAsync(DateTime.UtcNow.AddMinutes(2), 10, "test");
        Assert.Single(due);
    }

    [Fact]
    public async Task Cancel_prevents_delivery()
    {
        var store = new InMemoryScheduleStore();
        var token = await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            DeliverAt = DateTime.UtcNow.AddMinutes(-1),
            Destination = "q",
        });

        await store.CancelAsync(token);
        var due = await store.ClaimDueAsync(DateTime.UtcNow, 10, "test");
        Assert.Empty(due);
    }

    [Fact]
    public async Task Cron_upsert_and_claim_due_then_update_next_fire()
    {
        var store = new InMemoryScheduleStore();
        await store.UpsertCronAsync(new CronSchedule
        {
            Name = "daily",
            CronExpression = "0 6 * * *",
            MessageType = "m",
            NextFireAt = DateTime.UtcNow.AddMinutes(-1),
        });

        var due = await store.ClaimDueCronAsync(DateTime.UtcNow, "test");
        Assert.Single(due);

        var next = DateTime.UtcNow.AddDays(1);
        await store.UpdateCronAfterFireAsync(due[0].Id, DateTime.UtcNow, next);

        var again = await store.ClaimDueCronAsync(DateTime.UtcNow, "test");
        Assert.Empty(again);

        var listed = await store.ListCronAsync();
        Assert.Single(listed);
        Assert.Equal(next, listed[0].NextFireAt);
    }

    [Fact]
    public async Task Disabled_cron_is_not_claimed()
    {
        var store = new InMemoryScheduleStore();
        await store.UpsertCronAsync(new CronSchedule
        {
            Name = "off",
            CronExpression = "0 6 * * *",
            MessageType = "m",
            NextFireAt = DateTime.UtcNow.AddMinutes(-1),
            IsEnabled = false,
        });

        var due = await store.ClaimDueCronAsync(DateTime.UtcNow, "test");
        Assert.Empty(due);
    }
}
