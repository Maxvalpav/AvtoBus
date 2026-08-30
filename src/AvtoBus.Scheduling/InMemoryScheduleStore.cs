namespace AvtoBus.Scheduling;

/// <summary>
/// In-memory реализация <see cref="IScheduleStore"/> — эталон поведения и быстрые тесты без БД.
/// Задаёт семантику отложенной доставки и claim-ов. Не переживает рестарт.
/// </summary>
public sealed class InMemoryScheduleStore : IScheduleStore
{
    private readonly object _gate = new();

    private readonly List<ScheduledMessage> _scheduled = new();
    private readonly Dictionary<string, CronSchedule> _cron = new(StringComparer.Ordinal);
    private long _nextId = 1;

    public ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default)
    {
        lock (_gate)
        {
            if (message.Id == 0)
                message.Id = _nextId++;

            // Unique-джобы: дубликат с тем же ключом и не доставленный — игнорируем.
            if (message.UniqueKey is { } key)
            {
                var existing = _scheduled.FirstOrDefault(s =>
                    s.UniqueKey == key && s.DeliveredAt is null && s.CancelledAt is null);
                if (existing is not null)
                    return ValueTask.FromResult(existing.Token);
            }

            _scheduled.Add(message);
            return ValueTask.FromResult(message.Token);
        }
    }

    public ValueTask CancelAsync(Guid token, CancellationToken ct = default)
    {
        lock (_gate)
        {
            foreach (var m in _scheduled.Where(s => s.Token == token && s.DeliveredAt is null))
                m.CancelledAt = DateTime.UtcNow;
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(
        DateTime now, int batchSize, string claimedBy, CancellationToken ct = default)
    {
        List<ScheduledMessage> due;
        lock (_gate)
        {
            due = _scheduled
                .Where(s => s.DeliverAt <= now && s.DeliveredAt is null && s.CancelledAt is null)
                .OrderBy(s => s.DeliverAt)
                .Take(batchSize)
                .ToList();
        }
        return ValueTask.FromResult((IReadOnlyList<ScheduledMessage>)due);
    }

    public ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default)
    {
        var now = DateTime.UtcNow;
        lock (_gate)
        {
            foreach (var m in _scheduled.Where(s => ids.Contains(s.Id) && s.DeliveredAt is null))
                m.DeliveredAt = now;
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default)
    {
        lock (_gate)
        {
            var existing = _cron.TryGetValue(schedule.Name, out var prev);
            if (existing && prev is not null)
            {
                prev.CronExpression = schedule.CronExpression;
                prev.TimeZoneId = schedule.TimeZoneId;
                prev.MessageType = schedule.MessageType;
                prev.PayloadBlob = schedule.PayloadBlob;
                prev.NextFireAt = schedule.NextFireAt;
                prev.IsEnabled = schedule.IsEnabled;
                prev.Misfire = schedule.Misfire;
            }
            else
            {
                schedule.Id = _nextId++;
                _cron[schedule.Name] = schedule;
            }
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(
        DateTime now, string claimedBy, CancellationToken ct = default)
    {
        List<CronSchedule> due;
        lock (_gate)
        {
            due = _cron.Values
                .Where(c => c.IsEnabled && c.NextFireAt <= now)
                .OrderBy(c => c.NextFireAt)
                .ToList();
        }
        return ValueTask.FromResult((IReadOnlyList<CronSchedule>)due);
    }

    public ValueTask UpdateCronAfterFireAsync(
        long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default)
    {
        lock (_gate)
        {
            var cron = _cron.Values.FirstOrDefault(c => c.Id == id);
            if (cron is not null)
            {
                cron.LastFiredAt = firedAt;
                cron.NextFireAt = nextFireAt;
            }
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default)
    {
        List<CronSchedule> list;
        lock (_gate)
            list = _cron.Values.OrderBy(c => c.Name).Select(Clone).ToList();
        return ValueTask.FromResult((IReadOnlyList<CronSchedule>)list);
    }

    public ValueTask DeleteCronAsync(long id, CancellationToken ct = default)
    {
        lock (_gate)
        {
            var key = _cron.FirstOrDefault(kvp => kvp.Value.Id == id).Key;
            if (key is not null)
                _cron.Remove(key);
        }
        return ValueTask.CompletedTask;
    }

    private static CronSchedule Clone(CronSchedule c) => new()
    {
        Id = c.Id,
        Name = c.Name,
        CronExpression = c.CronExpression,
        TimeZoneId = c.TimeZoneId,
        MessageType = c.MessageType,
        PayloadBlob = (byte[])c.PayloadBlob.Clone(),
        LastFiredAt = c.LastFiredAt,
        NextFireAt = c.NextFireAt,
        IsEnabled = c.IsEnabled,
        Misfire = c.Misfire,
    };
}
