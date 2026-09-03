namespace AvtoBus.Outbox.EfCore;

/// <summary>Настройки outbox (док 15, §4, §7).</summary>
public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 200;
    public int Parallelism { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan CleanupAfter { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan StaleClaim { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// TTL партиционной лизы (аудит A1): сколько relay владеет PartitionKey без продления.
    /// Должен заведомо превышать время отправки+маркировки одной группы ключа, иначе
    /// другой инстанс перехватит ключ раньше (дубли поймает inbox-дедуп, порядок — head-of-line).
    /// </summary>
    public TimeSpan PartitionLeaseTtl { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Как часто на простое сверять pending/oldest с БД (аудит A3). Лёгкий COUNT-запрос.
    /// </summary>
    public TimeSpan HealthRefreshInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// После стольких неудачных попыток неотправленное сообщение считается poison
    /// и удаляется чисткой (иначе таблица растёт монотонно на вечно падающих).
    /// </summary>
    public int MaxPoisonAttempts { get; set; } = 25;

    /// <summary>
    /// Fail-fast валидация: невалидные значения раньше давали тихий stall релея
    /// (BatchSize=0 вечно возвращает 0, StaleClaim=0 вечно перезахватывает).
    /// </summary>
    public void Validate()
    {
        if (BatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "Outbox BatchSize должен быть >= 1.");
        if (Parallelism < 1)
            throw new ArgumentOutOfRangeException(nameof(Parallelism), "Outbox Parallelism должен быть >= 1.");
        if (PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(PollInterval), "Outbox PollInterval должен быть > 0.");
        if (StaleClaim <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(StaleClaim), "Outbox StaleClaim должен быть > 0.");
        if (PartitionLeaseTtl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(PartitionLeaseTtl), "Outbox PartitionLeaseTtl должен быть > 0.");
        if (HealthRefreshInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(HealthRefreshInterval), "Outbox HealthRefreshInterval должен быть > 0.");
        if (CleanupAfter <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(CleanupAfter), "Outbox CleanupAfter должен быть > 0.");
    }
}
