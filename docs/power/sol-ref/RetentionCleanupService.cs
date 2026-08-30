using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AvtoBus.Persistence.Postgres;

/// <summary>
/// Безопасная retention-очистка маленькими пачками согласно разделу 25.5
/// Outbox Sent старше 7 дней, Inbox старше 30 дней.
/// </summary>
public sealed class RetentionCleanupService : BackgroundService
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgresAvtoBusOptions _options;
    private readonly ILogger<RetentionCleanupService> _logger;

    public RetentionCleanupService(NpgsqlDataSource dataSource, IOptions<PostgresAvtoBusOptions> options, ILogger<RetentionCleanupService> logger)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Запускаем раз в час, внутри чистим батчами по 10k
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
        // Сразу при старте подождать 5 минут чтобы не конкурировать с миграциями
        try { await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken); } catch { return; }

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await CleanupOutboxAsync(stoppingToken);
                await CleanupInboxAsync(stoppingToken);
                await CleanupDlqAsync(stoppingToken);
                await CleanupProcessAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retention cleanup failed");
            }
        }
    }

    private async Task CleanupOutboxAsync(CancellationToken ct)
    {
        // Интервал берется из опций, дефолт 7 дней — соответствует разделу 25.5
        var interval = _options.RetentionOutboxSentAge;
        var days = Math.Max(1, (int)interval.TotalDays);
        // Fallback для субдневных интервалов: используем секунды
        var intervalLiteral = interval.TotalDays >= 1 ? $"{days} days" : $"{(int)interval.TotalSeconds} seconds";
        var sql = $"""
            WITH victim AS (
                SELECT event_id
                FROM avtobus.outbox_message
                WHERE status = 2
                  AND sent_at < clock_timestamp() - interval '{intervalLiteral}'
                ORDER BY sent_at
                LIMIT 10000
            )
            DELETE FROM avtobus.outbox_message AS o
            USING victim AS v
            WHERE o.event_id = v.event_id;
            """;
        var total = 0;
        while (!ct.IsCancellationRequested)
        {
            await using var conn = await _dataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, conn);
            var deleted = await cmd.ExecuteNonQueryAsync(ct);
            total += deleted;
            if (deleted < 10000) break;
        }
        if (total > 0) _logger.LogInformation("Outbox retention cleaned {Count} rows", total);
    }

    private async Task CleanupInboxAsync(CancellationToken ct)
    {
        var interval = _options.RetentionInboxAge;
        var days = Math.Max(1, (int)interval.TotalDays);
        var intervalLiteral = interval.TotalDays >= 1 ? $"{days} days" : $"{(int)interval.TotalSeconds} seconds";
        var sql = $"""
            WITH victim AS (
                SELECT consumer_name, event_source, event_id
                FROM avtobus.inbox_message
                WHERE processed_at < clock_timestamp() - interval '{intervalLiteral}'
                ORDER BY processed_at
                LIMIT 10000
            )
            DELETE FROM avtobus.inbox_message AS i
            USING victim AS v
            WHERE i.consumer_name = v.consumer_name
              AND i.event_source = v.event_source
              AND i.event_id = v.event_id;
            """;
        var total = 0;
        while (!ct.IsCancellationRequested)
        {
            await using var conn = await _dataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, conn);
            var deleted = await cmd.ExecuteNonQueryAsync(ct);
            total += deleted;
            if (deleted < 10000) break;
        }
        if (total > 0) _logger.LogInformation("Inbox retention cleaned {Count} rows", total);
    }

    private async Task CleanupDlqAsync(CancellationToken ct)
    {
        var interval = _options.RetentionDlqResolvedAge;
        var days = Math.Max(1, (int)interval.TotalDays);
        var intervalLiteral = interval.TotalDays >= 1 ? $"{days} days" : $"{(int)interval.TotalSeconds} seconds";
        var sql = $"""
            DELETE FROM avtobus.dead_letter
            WHERE status IN (1,2)
              AND resolved_at < clock_timestamp() - interval '{intervalLiteral}';
            """;
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        var deleted = await cmd.ExecuteNonQueryAsync(ct);
        if (deleted > 0) _logger.LogInformation("DLQ retention cleaned {Count} rows", deleted);
    }

    private async Task CleanupProcessAsync(CancellationToken ct)
    {
        var interval = _options.RetentionProcessExpiredAge;
        var days = Math.Max(1, (int)interval.TotalDays);
        var intervalLiteral = interval.TotalDays >= 1 ? $"{days} days" : $"{(int)interval.TotalSeconds} seconds";
        // Чистим completed старше интервала или is_completed=false но expires_at в прошлом (> интервала)
        var sql = $"""
            DELETE FROM avtobus.process_state
            WHERE (is_completed = true AND updated_at < clock_timestamp() - interval '{intervalLiteral}')
               OR (expires_at IS NOT NULL AND expires_at < clock_timestamp() - interval '{intervalLiteral}');
            """;
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        var deleted = await cmd.ExecuteNonQueryAsync(ct);
        if (deleted > 0) _logger.LogInformation("Process retention cleaned {Count} rows", deleted);
    }
}
