namespace AvtoBus.Configuration;

/// <summary>
/// Опции прод-пресета <see cref="ProductionDefaultsExtensions.UseProductionDefaults"/>:
/// вся надёжность в одном вызове вместо пяти.
/// </summary>
public sealed class ProductionOptions
{
    /// <summary>Профиль данных: Gdpr/Ru152Fz включает PII-маскирование.</summary>
    public DataProfile DataProfile { get; set; } = DataProfile.Default;

    /// <summary>
    /// Мастер-секрет подписи/шифрования (из Key Vault / K8s secrets).
    /// Применяется ТОЛЬКО полным пресетом <c>UseProductionDefaults&lt;TDb&gt;</c>
    /// (пакет Outbox.EfCore): базовый <c>UseProductionDefaults()</c> его игнорирует,
    /// безопасность конвертов без outbox-пакета не включается.
    /// Пусто — безопасность конвертов не включается (только надёжность доставки).
    /// </summary>
    public string MasterSecret { get; set; } = "";

    /// <summary>Соль PII-маски развёртки. Пусто — встроенный дефолт.</summary>
    public string PiiMaskSalt { get; set; } = "";

    /// <summary>
    /// Лимит исходящих сообщений в секунду. 0 — безлимит.
    /// Применяется только вместе с безопасностью (полный пресет).
    /// </summary>
    public int OutboundRatePerSecond { get; set; }

    /// <summary>Окно inbox-дедупликации.</summary>
    public TimeSpan InboxWindow { get; set; } = TimeSpan.FromHours(24);
}

/// <summary>
/// Прод-пресет в одном месте: recoverability + inbox-дедуп + circuit breaker +
/// лимиты заголовков. Полная версия с outbox и подписью —
/// <c>ProductionOutboxExtensions.UseProductionDefaults&lt;TDb&gt;</c> в AvtoBus.Outbox.EfCore.
/// </summary>
public static class ProductionDefaultsExtensions
{
    /// <summary>
    /// Надёжность без БД: 3 немедленных + 5 отложенных ретраев (экспонента от 5с),
    /// inbox-дедуп 24ч, circuit breaker 5/30с, лимиты заголовков.
    /// </summary>
    public static BusConfigurator UseProductionDefaults(
        this BusConfigurator bus, Action<ProductionOptions>? configure = null)
    {
        var opts = new ProductionOptions();
        configure?.Invoke(opts);
        ApplyCore(bus, opts);
        return bus;
    }

    /// <summary>Ядро пресета (для переиспользования полными пресетами других пакетов).</summary>
    public static void ApplyCore(BusConfigurator bus, ProductionOptions opts)
    {
        bus.Recoverability(r => r.ImmediateRetries(3).DelayedRetries(5));
        bus.UseInboxDeduplication(opts.InboxWindow);
        bus.UseCircuitBreaker(5, TimeSpan.FromSeconds(30));
        bus.UseHeaderLimits();
        if (!string.IsNullOrEmpty(opts.PiiMaskSalt))
            bus.PiiMaskSalt = opts.PiiMaskSalt;
        if (opts.DataProfile is not DataProfile.Default)
            bus.UseDataProfile(opts.DataProfile);
    }
}
