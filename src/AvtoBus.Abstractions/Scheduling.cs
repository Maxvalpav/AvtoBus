namespace AvtoBus.Abstractions;

public enum ScheduledState { Scheduled, Claimed, Dispatched, Cancelled }

public sealed class ScheduledRecord
{
    public required Guid Id { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public required string Destination { get; init; }
    public required string Transport { get; init; }
    public required DateTimeOffset ScheduledAt { get; init; }
    public ScheduledState State { get; set; } = ScheduledState.Scheduled;
    public string? LockedBy { get; set; }
    public DateTimeOffset? LockedUntil { get; set; }
    public int AttemptCount { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
}

public interface IScheduledStore
{
    ValueTask AddAsync(IReadOnlyList<ScheduledRecord> records, CancellationToken ct);
    ValueTask<IReadOnlyList<ScheduledRecord>> ClaimDueAsync(int batchSize, string claimedBy, TimeSpan lockDuration, CancellationToken ct);
    ValueTask MarkDispatchedAsync(IReadOnlyList<Guid> ids, CancellationToken ct);
    ValueTask MarkFailedAsync(Guid id, string error, DateTimeOffset nextAttempt, CancellationToken ct);
    ValueTask CancelAsync(Guid id, CancellationToken ct);
    ValueTask<int> CountDueAsync(DateTimeOffset now, CancellationToken ct);
}

public interface IAvtoScheduler
{
    ValueTask<Guid> ScheduleAsync(object message, TimeSpan delay, CancellationToken ct = default);
    ValueTask<Guid> ScheduleAtAsync(object message, DateTimeOffset at, CancellationToken ct = default);
    ValueTask CancelAsync(Guid scheduleId, CancellationToken ct = default);
}
