namespace AvtoBus.RabbitMq;

/// <summary>
/// Настройки RabbitMQ-транспорта (идеи 61–62): connection string, топология,
/// доставка и DLQ. Очереди по умолчанию — quorum (диск, HA, x-delivery-count).
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>Connection string, напр. <c>amqp://guest:guest@localhost:5672/</c>.</summary>
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>Имя клиента, видимое в management-плагине.</summary>
    public string ClientProvidedName { get; set; } = "avtobus";

    /// <summary>Heartbeat соединения: брокер закрывает мёртвые соединения быстрее.</summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Пауза между попытками сетевого восстановления (topology recovery включён).</summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Quorum queues (идея 62): диск, replicate между узлами, нативные счётчики доставки
    /// (<c>x-delivery-count</c>) и <c>x-delivery-limit</c>. Рекомендуется для продакшена.
    /// </summary>
    public bool UseQuorumQueues { get; set; } = true;

    /// <summary>
    /// Предел доставок сообщения (идея 164): после N попыток оно уходит в DLQ
    /// (или выбрасывается, если <see cref="UseDeadLetterExchange"/> выключен).
    /// </summary>
    public int DeliveryLimit { get; set; } = 6;

    /// <summary>
    /// Dead letter exchange для переполнившихся и отвергнутых без requeue сообщений.
    /// Для каждой очереди <c>foo</c> создаются exchange <c>foo.dlx</c> и очередь <c>foo.dlq</c>.
    /// </summary>
    public bool UseDeadLetterExchange { get; set; } = true;

    /// <summary>
    /// Топики реализованы на stream-очередях (логи с retention, как Kafka): сообщение, опубликованное
    /// до появления консьюмера, не теряется — каждый консьюмер-группы читает лог с начала
    /// (<c>x-stream-offset: first</c>). Предел размера лога на топик (байт).
    /// </summary>
    public long TopicRetentionMaxBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    /// Таймаут publisher confirm. Если брокер не подтвердил публикацию за это время —
    /// бросается исключение, сообщение не считается отправленным.
    /// </summary>
    public TimeSpan PublishConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
