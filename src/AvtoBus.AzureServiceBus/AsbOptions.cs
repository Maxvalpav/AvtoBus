namespace AvtoBus.AzureServiceBus;

/// <summary>
/// Настройки Azure Service Bus-транспорта (идеи 61–62): сессии для строгого порядка,
/// scheduled enqueue для отложенных, авто-продление lock-а долгих обработок.
/// </summary>
public sealed class AsbOptions
{
    /// <summary>Строка подключения, либо адрес вида <c>Endpoint=...;SharedAccessKeyName=...;SharedAccessKey=...</c>.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Имя приложения — суффикс имён подписок топиков и консьюмеров.</summary>
    public string Name { get; set; } = "avtobus";

    /// <summary>Prefetch: сколько сообщений ASB отдаёт консьюмеру до подтверждения.</summary>
    public int PrefetchCount { get; set; } = 32;

    /// <summary>
    /// Сессии (идея 61): включить requires-session на очередях/топиках. PartitionKey конверта
    /// маппится на SessionId — строгий порядок внутри сессии.
    /// </summary>
    public bool RequireSessions { get; set; }

    /// <summary>Максимальное продление lock-а долгого обработчика (идея 62).</summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Таймаут ожидания сессии, если сессии заняты (SessionIdleTimeout).</summary>
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Время жизни сообщения по умолчанию (0 — наследуется от очереди/топика).</summary>
    public TimeSpan? DefaultMessageTimeToLive { get; set; }
}
