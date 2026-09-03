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
        if (CleanupAfter <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(CleanupAfter), "Outbox CleanupAfter должен быть > 0.");
    }
}
