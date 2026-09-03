using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Outbox.EfCore;

/// <summary>Периодическая чистка отправленных outbox и старых inbox по TTL (док 15, §7).</summary>
public sealed class OutboxCleanup : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly OutboxOptions _opt;
    private readonly ILogger<OutboxCleanup> _log;

    public OutboxCleanup(IServiceScopeFactory scopes, OutboxOptions opt, ILogger<OutboxCleanup> log)
    {
        _scopes = scopes;
        _opt = opt;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await using var scope = _scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<DbContext>();
                var timeProvider = scope.ServiceProvider.GetService<TimeProvider>() ?? TimeProvider.System;
                var cutoff = timeProvider.GetUtcNow().UtcDateTime - _opt.CleanupAfter;

                await DeleteExpiredAsync(db, cutoff, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (IsMissingTable(ex))
            {
                // Гонка со SchemaMigrator на старте: таблицы ещё нет — не умираем
                // (смерть фоновой задачи останавливает весь хост), пробуем позже.
                _log.LogDebug(ex, "OutboxCleanup: схема ещё не готова, повтор позже.");
            }
            catch (Exception ex)
            {
                // Чистка — необязательная maintenance-задача: её смерть не должна
                // ронять хост (дефолтное поведение BackgroundService — StopHost,
                // что маскируется под OperationCanceledException в других сервисах).
                _log.LogWarning(ex, "OutboxCleanup: пропуск цикла чистки.");
            }

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

    /// <summary>Таблица ещё не создана (гонка со SchemaMigrator): PG 42P01, SQLite «no such table».</summary>
    internal static bool IsMissingTable(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e.GetType().Name == "PostgresException" &&
                e.GetType().GetProperty("SqlState")?.GetValue(e) as string == "42P01")
                return true;
            var msg = e.Message;
            if (msg.Contains("42P01", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("no such table", StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}
