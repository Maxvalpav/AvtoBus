using AvtoBus;

namespace AvtoBus.Scheduling;

/// <summary>
/// Отложенное сообщение в durable-хранилище (идеи 226, 46).
/// </summary>
public sealed class ScheduledMessage
{
    public long Id { get; set; }
    public Guid Token { get; set; }
    public string MessageType { get; set; } = "";
    public byte[] EnvelopeBlob { get; set; } = [];
    public string Destination { get; set; } = "";
    public string Transport { get; set; } = "";
    public DateTime DeliverAt { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? DeliveredAt { get; set; }
    public DateTime? CancelledAt { get; set; }
    public string? UniqueKey { get; set; }
    public string? TenantId { get; set; }
}

/// <summary>Cron-расписание (идея 223).</summary>
public sealed class CronSchedule
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string CronExpression { get; set; } = "";
    public string TimeZoneId { get; set; } = "UTC";
    public string MessageType { get; set; } = "";
    public byte[] PayloadBlob { get; set; } = [];
    public DateTime? LastFiredAt { get; set; }
    public DateTime NextFireAt { get; set; }
    public bool IsEnabled { get; set; } = true;
    public MisfirePolicy Misfire { get; set; } = MisfirePolicy.FireLatest;
}

/// <summary>Политика пропущенных срабатываний (идея 223).</summary>
public enum MisfirePolicy
{
    /// <summary>Отработать все пропущенные срабатывания.</summary>
    FireAll,
    /// <summary>Отработать только последнее пропущенное.</summary>
    FireLatest,
    /// <summary>Пропустить и ждать следующего по расписанию.</summary>
    Skip,
}
