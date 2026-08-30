using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Outbox.EfCore;

/// <summary>Периодическая чистка отправленных outbox и старых inbox по TTL (док 15, §7).</summary>
public sealed class OutboxCleanup : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly OutboxOptions _opt;

    public OutboxCleanup(IServiceScopeFactory scopes, OutboxOptions opt)
    {
        _scopes = scopes;
        _opt = opt;
    }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            await using var scope = _scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DbContext>();
            var timeProvider = scope.ServiceProvider.GetService<TimeProvider>() ?? TimeProvider.System;
            var cutoff = timeProvider.GetUtcNow().UtcDateTime - _opt.CleanupAfter;

            await DeleteExpiredAsync(db, cutoff, stop).ConfigureAwait(false);

            try { await Task.Delay(TimeSpan.FromMinutes(15), stop).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }

    /// <summary>
    /// Удаляет отправленные outbox и устаревшие inbox-записи старше <paramref name="cutoff"/>.
    /// Вынесено отдельно, чтобы retention-поведение можно было проверить тестом без фонового цикла.
    /// </summary>
    public static async Task DeleteExpiredAsync(DbContext db, DateTime cutoff, CancellationToken ct)
    {
        await db.Set<OutboxMessage>()
            .Where(o => o.SentAt != null && o.SentAt < cutoff)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);

        await db.Set<InboxRecord>()
            .Where(i => i.ProcessedAt < cutoff)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);
    }
}
