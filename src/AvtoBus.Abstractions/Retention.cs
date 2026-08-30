namespace AvtoBus.Abstractions;

public sealed class AvtoRetentionOptions
{
    public TimeSpan OutboxRetention { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan InboxRetention { get; set; } = TimeSpan.FromDays(30);
    public TimeSpan DeadLetterRetention { get; set; } = TimeSpan.FromDays(90);
    public TimeSpan ScheduledRetention { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan EventStreamRetention { get; set; } = TimeSpan.Zero; // 0 = forever (event sourcing)
    public int BatchSize { get; set; } = 1000;
}

public interface IAvtoRetentionStore
{
    ValueTask<int> CleanupOutboxAsync(DateTimeOffset cutoff, int batchSize, CancellationToken ct);
    ValueTask<int> CleanupInboxAsync(DateTimeOffset cutoff, int batchSize, CancellationToken ct);
    ValueTask<int> CleanupDeadLettersAsync(DateTimeOffset cutoff, int batchSize, CancellationToken ct);
    ValueTask<int> CleanupScheduledAsync(DateTimeOffset cutoff, int batchSize, CancellationToken ct);
    ValueTask<int> CleanupEventStreamsAsync(DateTimeOffset cutoff, int batchSize, CancellationToken ct);
}
