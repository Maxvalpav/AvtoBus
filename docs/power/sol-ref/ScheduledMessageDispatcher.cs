using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Persistence.Postgres;

public sealed class ScheduledMessageDispatcher : BackgroundService
{
    private const string MoveDueSql = """
        WITH due AS MATERIALIZED (
            SELECT schedule_id
            FROM avtobus.scheduled_message
            WHERE status = 0
              AND due_at <= clock_timestamp()
            ORDER BY due_at, schedule_id
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED
        ), inserted AS (
            INSERT INTO avtobus.outbox_message (
                event_id, event_source, event_type, subject, partition_key,
                destination, content_type, envelope, envelope_sha256,
                transport_headers, available_at)
            SELECT
                s.event_id, s.event_source, s.event_type, s.subject, s.partition_key,
                s.destination, s.content_type, s.envelope, s.envelope_sha256,
                s.transport_headers, clock_timestamp()
            FROM avtobus.scheduled_message AS s
            JOIN due AS d ON d.schedule_id = s.schedule_id
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        )
        UPDATE avtobus.scheduled_message AS s
        SET status = 2,
            enqueued_at = clock_timestamp()
        WHERE s.event_id IN (SELECT event_id FROM inserted)
        RETURNING s.schedule_id;
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly IOutboxSignal _outboxSignal;
    private readonly IScheduledSignal _scheduledSignal;
    private readonly PostgresAvtoBusOptions _options;

    public ScheduledMessageDispatcher(
        NpgsqlDataSource dataSource,
        IOutboxSignal outboxSignal,
        IScheduledSignal scheduledSignal,
        IOptions<PostgresAvtoBusOptions> options)
    {
        _dataSource = dataSource;
        _outboxSignal = outboxSignal;
        _scheduledSignal = scheduledSignal;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var moved = await MoveBatchAsync(stoppingToken);
                if (moved > 0)
                {
                    _outboxSignal.Pulse();
                    if (moved == _options.BatchSize) continue;
                }

                await WaitForSignalOrDelayAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task WaitForSignalOrDelayAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var signalTask = _scheduledSignal.WaitAsync(linked.Token).AsTask();
        var delayTask = Task.Delay(_options.IdlePollingInterval, linked.Token);
        try
        {
            await Task.WhenAny(signalTask, delayTask);
        }
        finally
        {
            await linked.CancelAsync();
            try { await Task.WhenAll(signalTask, delayTask); }
            catch (OperationCanceledException) { }
        }
    }

    private async ValueTask<int> MoveBatchAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(MoveDueSql, connection, transaction);
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, _options.BatchSize);

        var count = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) count++;
        await reader.DisposeAsync();
        await transaction.CommitAsync(cancellationToken);
        return count;
    }
}
