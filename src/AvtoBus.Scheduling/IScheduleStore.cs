namespace AvtoBus.Scheduling;

/// <summary>
/// Хранилище отложенных сообщений и cron-расписаний (идеи 223, 226).
/// Не привязано к конкретной БД: реализуется в памяти (InMemoryScheduleStore)
/// и на EF Core (EfCoreScheduleStore&lt;TDb&gt;).
/// </summary>
public interface IScheduleStore
{
    ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default);
    ValueTask CancelAsync(Guid token, CancellationToken ct = default);
    ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(
        DateTime now, int batchSize, string claimedBy, CancellationToken ct = default);
    ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default);

    // Cron

    ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default);
    ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(
        DateTime now, string claimedBy, CancellationToken ct = default);
    ValueTask UpdateCronAfterFireAsync(
        long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default);
    ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default);
    ValueTask DeleteCronAsync(long id, CancellationToken ct = default);
}
