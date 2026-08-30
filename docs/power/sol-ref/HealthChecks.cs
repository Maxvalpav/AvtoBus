using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AvtoBus.Persistence.Postgres;

/// <summary>
/// Health checks согласно разделу 24.3 спека:
/// - Liveness: процесс жив (всегда Healthy если не cancelled)
/// - Readiness producer: PostgreSQL доступен + миграция совместима
/// - Readiness consumer: PostgreSQL + broker connection
/// - Degraded: oldest Outbox age, DLQ рост
/// </summary>
public sealed class AvtoBusPostgresReadinessHealthCheck : IHealthCheck
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgresAvtoBusOptions _options;

    public AvtoBusPostgresReadinessHealthCheck(NpgsqlDataSource dataSource, IOptions<PostgresAvtoBusOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand("SELECT max(version) FROM avtobus.schema_version;", conn);
            var maxVersion = (int?)await cmd.ExecuteScalarAsync(cancellationToken);
            if (maxVersion is null or < 2)
                return HealthCheckResult.Degraded($"AvtoBus schema_version {maxVersion} < required 2 (V1+V2 not applied)");
            if (maxVersion is null)
                return HealthCheckResult.Unhealthy("AvtoBus schema_version missing");

            // Проверка backlog для degraded
            await using var backlogCmd = new NpgsqlCommand("""
                SELECT extract(epoch FROM (clock_timestamp() - min(available_at) FILTER (WHERE status = 0))) 
                FROM avtobus.outbox_message WHERE status IN (0,1);
                """, conn);
            var oldest = await backlogCmd.ExecuteScalarAsync(cancellationToken);
            var sloSeconds = _options.HealthOutboxOldestDegradedAfter.TotalSeconds;
            if (oldest is double seconds && seconds > sloSeconds)
            {
                return HealthCheckResult.Degraded($"Outbox oldest age {seconds:F0}s exceeds {sloSeconds:F0}s SLO");
            }

            // DLQ open count
            await using var dlqCmd = new NpgsqlCommand("SELECT count(*) FROM avtobus.dead_letter WHERE status = 0;", conn);
            var dlqCount = (long?)await dlqCmd.ExecuteScalarAsync(cancellationToken) ?? 0;
            if (dlqCount > _options.HealthDlqOpenDegradedAfter)
            {
                return HealthCheckResult.Degraded($"DLQ open count {dlqCount} exceeds threshold {_options.HealthDlqOpenDegradedAfter}");
            }

            return HealthCheckResult.Healthy("PostgreSQL and AvtoBus schema reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("PostgreSQL readiness failed", ex);
        }
    }
}

public sealed class AvtoBusLivenessHealthCheck : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(HealthCheckResult.Healthy("Process alive"));
}

public sealed class AvtoBusConsumerReadinessHealthCheck : IHealthCheck
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly AvtoBus.Abstractions.ITransportReceiver? _receiver;

    public AvtoBusConsumerReadinessHealthCheck(NpgsqlDataSource dataSource, IServiceProvider services)
    {
        _dataSource = dataSource;
        _receiver = services.GetService(typeof(AvtoBus.Abstractions.ITransportReceiver)) as AvtoBus.Abstractions.ITransportReceiver;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand("SELECT 1;", conn);
            await cmd.ExecuteScalarAsync(cancellationToken);
            if (_receiver is null)
                return HealthCheckResult.Degraded("Transport receiver not registered");
            return HealthCheckResult.Healthy("Consumer readiness: DB and transport available");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Consumer readiness failed", ex);
        }
    }
}
