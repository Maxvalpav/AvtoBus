using AvtoBus;
using AvtoBus.Observability;
using AvtoBus.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Scheduling;

/// <summary>
/// Фоновый сервис: доставляет отложенные сообщения и запускает cron-джобы (идея 223).
/// Cron защищён leader election — только одна реплика в кластере фаерит (идея 224).
/// </summary>
public sealed class SchedulerService : BackgroundService
{
    private readonly IScheduleStore _store;
    private readonly IEnvelopeFactory _envelopes;
    private readonly ILeaderElection _leader;
    private readonly TransportRegistry _transports;
    private readonly SchedulerOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<SchedulerService> _log;
    private readonly string _instanceId = $"{Environment.MachineName}/{Environment.ProcessId}";

    public SchedulerService(
        IScheduleStore store,
        TransportRegistry transports,
        IEnvelopeFactory envelopes,
        ILeaderElection leader,
        SchedulerOptions options,
        TimeProvider clock,
        ILogger<SchedulerService> log)
    {
        _store = store;
        _transports = transports;
        _envelopes = envelopes;
        _leader = leader;
        _options = options;
        _clock = clock;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var delayedTask = RunDelayedLoopAsync(ct);
        var cronTask = RunCronLoopAsync(ct);
        await Task.WhenAll(delayedTask, cronTask);
    }

    // ── Отложенные сообщения (могут обрабатывать все реплики) ──

    private async Task RunDelayedLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var now = _clock.GetUtcNow().UtcDateTime;
                var due = await _store.ClaimDueAsync(now, _options.BatchSize, _instanceId, ct);

                if (due.Count == 0)
                {
                    await Task.Delay(_options.PollInterval, ct);
                    continue;
                }

                var delivered = new List<long>(due.Count);

                foreach (var msg in due)
                {
                    try
                    {
                        var envelope = _envelopes.Deserialize(msg.EnvelopeBlob);
                        var transport = _transports.Get(msg.Transport.Length == 0 ? null : msg.Transport);
                        await transport.SendAsync(
                            envelope with { DeliverAt = null, SentAt = _clock.GetUtcNow() },
                            TransportDestination.Queue(msg.Destination),
                            ct);
                        delivered.Add(msg.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Failed to deliver scheduled message {Token}", msg.Token);
                    }
                }

                BusTelemetry.ScheduledDeliveredCount.Add(delivered.Count);
                await _store.MarkDeliveredAsync(delivered, ct);
                _log.LogDebug("Delivered {Count} scheduled messages", delivered.Count);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler delayed-loop error");
                try { await Task.Delay(_options.ErrorDelay, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    // ── Cron (только лидер) ──

    private async Task RunCronLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await _leader.TryAcquireAsync("avtobus-cron", _options.LeaderLease, ct))
                {
                    await Task.Delay(_options.LeaderRetryInterval, ct);
                    continue;
                }

                _log.LogInformation("Acquired cron leadership: {Instance}", _instanceId);

                while (!ct.IsCancellationRequested && await _leader.RenewAsync("avtobus-cron", _options.LeaderLease, ct))
                {
                    await FireDueCronAsync(ct);
                    await Task.Delay(_options.CronPollInterval, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler cron-loop error");
                try { await Task.Delay(_options.ErrorDelay, ct); }
                catch (OperationCanceledException) { break; }
            }
        }

        await _leader.ReleaseAsync("avtobus-cron", CancellationToken.None);
    }

    private async Task FireDueCronAsync(CancellationToken ct)
    {
        var now = _clock.GetUtcNow();
        var due = await _store.ClaimDueCronAsync(now.UtcDateTime, _instanceId, ct);

        foreach (var schedule in due)
        {
            try
            {
                var cron = CronExpression.Parse(schedule.CronExpression);
                var tz = TimeZoneInfo.FindSystemTimeZoneById(schedule.TimeZoneId);

                // Misfire-политика
                var fireCount = schedule.Misfire switch
                {
                    MisfirePolicy.FireAll => CountMissed(cron, schedule, now, tz),
                    MisfirePolicy.FireLatest => 1,
                    MisfirePolicy.Skip => 0,
                    _ => 1
                };

                for (var i = 0; i < fireCount; i++)
                {
                    var envelope = _envelopes.Deserialize(schedule.PayloadBlob);
                    var transport = _transports.Default;
                    await transport.SendAsync(
                        envelope with { MessageId = Guid.NewGuid(), SentAt = now, DeliverAt = null },
                        TransportDestination.Topic(schedule.MessageType),
                        ct);
                }

                var next = cron.GetNextOccurrence(now, tz) ?? now.AddYears(1);

                await _store.UpdateCronAfterFireAsync(schedule.Id, now.UtcDateTime, next.UtcDateTime, ct);

                BusTelemetry.CronFiredCount.Add(fireCount);
                _log.LogInformation("Cron '{Name}' fired {Count}x, next at {Next}",
                    schedule.Name, fireCount, next);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Cron '{Name}' failed", schedule.Name);
            }
        }
    }

    private static int CountMissed(CronExpression cron, CronSchedule schedule, DateTimeOffset now, TimeZoneInfo tz)
    {
        if (schedule.LastFiredAt is not { } last)
            return 1;
        var count = 0;
        var cursor = new DateTimeOffset(last, TimeSpan.Zero);
        while (count < 100)
        {
            var next = cron.GetNextOccurrence(cursor, tz);
            if (next is null || next > now) break;
            count++;
            cursor = next.Value;
        }
        return Math.Max(1, count);
    }
}

public sealed class SchedulerOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan CronPollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan LeaderLease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(10);
}
