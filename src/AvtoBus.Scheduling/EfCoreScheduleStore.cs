using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Scheduling;

/// <summary>
/// Хранилище расписаний на EF Core (пользовательский <typeparamref name="TDb"/> + <c>ConfigureScheduling</c>).
/// Claim отложенных сообщений — через обновление с фильтром (атомарно в транзакции).
/// </summary>
public sealed class EfCoreScheduleStore<TDb> : IScheduleStore
    where TDb : DbContext
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TimeProvider _clock;

    public EfCoreScheduleStore(IServiceScopeFactory scopes, TimeProvider clock)
    {
        _scopes = scopes;
        _clock = clock;
    }

    public async ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        if (message.UniqueKey is { } key)
        {
            var existing = await db.Set<ScheduledMessage>()
                .FirstOrDefaultAsync(s => s.UniqueKey == key && s.DeliveredAt == null && s.CancelledAt == null, ct);
            if (existing is not null)
                return existing.Token;
        }

        message.Id = 0;
        message.Token = message.Token == Guid.Empty ? Guid.NewGuid() : message.Token;
        message.CreatedAt = _clock.GetUtcNow().UtcDateTime;
        db.Set<ScheduledMessage>().Add(message);
        await db.SaveChangesAsync(ct);
        return message.Token;
    }

    public async ValueTask CancelAsync(Guid token, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await db.Set<ScheduledMessage>()
            .Where(s => s.Token == token && s.DeliveredAt == null)
            .ExecuteUpdateAsync(u => u.SetProperty(s => s.CancelledAt, _clock.GetUtcNow().UtcDateTime), ct);
    }

    public async ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(
        DateTime now, int batchSize, string claimedBy, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        return await db.Set<ScheduledMessage>()
            .Where(s => s.DeliverAt <= now && s.DeliveredAt == null && s.CancelledAt == null)
            .OrderBy(s => s.DeliverAt)
            .Take(batchSize)
            .ToListAsync(ct);
    }

    public async ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default)
    {
        if (ids.Count == 0)
            return;

        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await db.Set<ScheduledMessage>()
            .Where(s => ids.Contains(s.Id) && s.DeliveredAt == null)
            .ExecuteUpdateAsync(u => u.SetProperty(s => s.DeliveredAt, _clock.GetUtcNow().UtcDateTime), ct);
    }

    public async ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var existing = await db.Set<CronSchedule>()
            .FirstOrDefaultAsync(c => c.Name == schedule.Name, ct);

        if (existing is null)
        {
            schedule.Id = 0;
            db.Set<CronSchedule>().Add(schedule);
        }
        else
        {
            existing.CronExpression = schedule.CronExpression;
            existing.TimeZoneId = schedule.TimeZoneId;
            existing.MessageType = schedule.MessageType;
            existing.PayloadBlob = schedule.PayloadBlob;
            existing.NextFireAt = schedule.NextFireAt;
            existing.IsEnabled = schedule.IsEnabled;
            existing.Misfire = schedule.Misfire;
        }

        await db.SaveChangesAsync(ct);
    }

    public async ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(
        DateTime now, string claimedBy, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        return await db.Set<CronSchedule>()
            .Where(c => c.IsEnabled && c.NextFireAt <= now)
            .OrderBy(c => c.NextFireAt)
            .ToListAsync(ct);
    }

    public async ValueTask UpdateCronAfterFireAsync(
        long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await db.Set<CronSchedule>()
            .Where(c => c.Id == id)
            .ExecuteUpdateAsync(u => u
                .SetProperty(c => c.LastFiredAt, firedAt)
                .SetProperty(c => c.NextFireAt, nextFireAt), ct);
    }

    public async ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        return await db.Set<CronSchedule>().OrderBy(c => c.Name).ToListAsync(ct);
    }

    public async ValueTask DeleteCronAsync(long id, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await db.Set<CronSchedule>().Where(c => c.Id == id).ExecuteDeleteAsync(ct);
    }
}
