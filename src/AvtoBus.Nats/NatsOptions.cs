namespace AvtoBus.Nats;

/// <summary>
/// Настройки NATS/JetStream-транспорта (идеи 63–64). URL сервера, тюнинг стримов,
/// back-pressure через batch fetch pull-consumers.
/// </summary>
public sealed class NatsOptions
{
    /// <summary>Адрес сервера: <c>nats://localhost:4222</c>, через запятую для кластера.</summary>
    public string Url { get; set; } = "nats://localhost:4222";

    /// <summary>Базовое имя приложения — используется в именах consumer-ов по умолчанию.</summary>
    public string Name { get; set; } = "avtobus";

    /// <summary>Хранение JetStream стрима: file (durable) или memory (быстро, не переживает рестарт).</summary>
    public string StorageType { get; set; } = "file";

    /// <summary>Сколько сообщений держать в стриме на subject (0 — без лимита).</summary>
    public long MaxMsgsPerStream { get; set; } = 0;

    /// <summary>Максимальный возраст сообщений в стриме (0 — без лимита).</summary>
    public TimeSpan MaxAge { get; set; } = TimeSpan.FromDays(1);

    /// <summary>Количество реплик стрима (1 — без репликации).</summary>
    public int Replicas { get; set; } = 1;

    /// <summary>
    /// Размер batch-а pull-consumer'а. Один fetch вытягивает до этого числа сообщений —
    /// идеальный back-pressure (идея 63).
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>Таймаут fetch-а pull-consumer'а: как долго ждём хотя бы одно сообщение.</summary>
    public TimeSpan FetchTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Максимум попыток доставки до отказа (маппится на JetStream MaxDeliver).</summary>
    public int MaxDeliver { get; set; } = 100;

    /// <summary>Таймаут подтверждения: не-acked сообщение вернётся в стрим (AckWait).</summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Использовать очередь консьюмеров: делить subject между конкурентными воркерами.</summary>
    public bool UseQueueGroups { get; set; } = true;
}
