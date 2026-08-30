using System.Data;

namespace AvtoBus.Persistence.Postgres;

public sealed class PostgresAvtoBusOptions
{
    public int BatchSize { get; set; } = 100;
    public int MaxPublishConcurrency { get; set; } = 16;
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan IdlePollingInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxEnvelopeBytes { get; set; } = 1024 * 1024;
    public int MaxConsumerDeliveryAttempts { get; set; } = 10;
    public int MaxConsumerConcurrency { get; set; } = 8;
    public TimeSpan RetentionOutboxSentAge { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan RetentionInboxAge { get; set; } = TimeSpan.FromDays(30);
    public TimeSpan RetentionDlqResolvedAge { get; set; } = TimeSpan.FromDays(90);
    public TimeSpan RetentionProcessExpiredAge { get; set; } = TimeSpan.FromDays(30);
    public TimeSpan HealthOutboxOldestDegradedAfter { get; set; } = TimeSpan.FromSeconds(300);
    public long HealthDlqOpenDegradedAfter { get; set; } = 100;
    public string WorkerId { get; set; } =
        $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";

    public void Validate()
    {
        if (BatchSize is < 1 or > 10_000) throw new InvalidOperationException("Invalid BatchSize.");
        if (MaxPublishConcurrency is < 1 or > 1_000) throw new InvalidOperationException("Invalid concurrency.");
        if (MaxConsumerConcurrency is < 1 or > 64) throw new InvalidOperationException("Invalid consumer concurrency.");
        if (LeaseDuration < TimeSpan.FromSeconds(5)) throw new InvalidOperationException("Lease is too short.");
        if (MaxEnvelopeBytes < 1024) throw new InvalidOperationException("MaxEnvelopeBytes is too small.");
        if (MaxConsumerDeliveryAttempts < 1) throw new InvalidOperationException("Invalid delivery attempts.");
        if (HealthDlqOpenDegradedAfter < 0) throw new InvalidOperationException("Invalid DLQ threshold.");
    }
}
