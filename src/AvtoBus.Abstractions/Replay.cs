namespace AvtoBus.Abstractions;

public sealed record ReplayRequest(
    Guid DeadLetterId,
    string Reason,
    string AuthorizedBy,
    bool DryRun = false);

public sealed record ReplayResult(
    bool Success,
    Guid? NewMessageId = null,
    string? Error = null);

public sealed record BackfillOptions(
    string Consumer,
    string Strategy, // from-now, from-beginning, from-offset, from-timestamp, from-snapshot
    DateTimeOffset? FromTimestamp = null,
    string? FromOffset = null,
    int BatchSize = 500,
    int MaxConcurrency = 4,
    int RateLimitPerSecond = 1000);

public sealed record BackfillProgress(
    string Consumer,
    long Processed,
    long Total,
    double Percent,
    string LastPosition);

public interface IDeadLetterReplayer
{
    ValueTask<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct);
    ValueTask<IReadOnlyList<ReplayResult>> ReplayBulkAsync(string query, string reason, string authorizedBy, CancellationToken ct);
    ValueTask<BackfillProgress> BackfillAsync(BackfillOptions options, CancellationToken ct);
}
