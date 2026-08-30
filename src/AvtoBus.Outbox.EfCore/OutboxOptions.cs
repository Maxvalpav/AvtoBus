namespace AvtoBus.Outbox.EfCore;

/// <summary>Настройки outbox (док 15, §4, §7).</summary>
public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 200;
    public int Parallelism { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan CleanupAfter { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan StaleClaim { get; set; } = TimeSpan.FromMinutes(2);
}
