using Confluent.Kafka;

namespace AvtoBus.Kafka;

/// <summary>
/// Настройки Kafka-транспорта (идеи 57–60). Bootstrap-серверы, семантика доставки,
/// дефолтная топология топиков.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>Адреса брокеров: <c>localhost:9092</c>, через запятую.</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Базовое имя клиента: к нему добавляется суффикс producer/consumer/group.</summary>
    public string ClientId { get; set; } = "avtobus";

    /// <summary>Количество партиций топиков, создаваемых в ProvisionAsync.</summary>
    public int DefaultPartitions { get; set; } = 6;

    /// <summary>Коэффициент репликации топиков, создаваемых в ProvisionAsync.</summary>
    public short DefaultReplicationFactor { get; set; } = 1;

    /// <summary>Группа консьюмеров по умолчанию — обычно имя сервиса.</summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Exactly-once через транзакции продюсера (идея 57): <c>transactional.id</c>,
    /// <c>enable.idempotence</c>, оффсеты коммитятся в транзакции, консьюмеры читают
    /// только закоммиченные (<c>isolation.level=read_committed</c>).
    /// </summary>
    public bool ExactlyOnce { get; set; }

    /// <summary>
    /// При exactly-once — уникальный идентификатор транзакционного продюсера на инстанс.
    /// Один и тот же id не должен делиться между процессами (лидер-элекшн).
    /// </summary>
    public string TransactionalId { get; set; } = "avtobus";

    /// <summary>Подтверждение продюсера: 0, 1 или -1 (all). По умолчанию all.</summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>Сжатие тела: none, gzip, snappy, lz4, zstd.</summary>
    public CompressionType? CompressionType { get; set; } = Confluent.Kafka.CompressionType.Lz4;

    /// <summary>
    /// Начать чтение топика: <c>earliest</c> (с самого начала) или <c>latest</c>.
    /// Для команд — earliest (не терять, пока нет группы); для событий часто latest.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>Максимальный размер пакета продюсера (байт).</summary>
    public int MaxMessageBytes { get; set; } = 1_000_000;

    /// <summary>
    /// Пока остановить/возобновлять партиции при back-pressure (идея 59). Включает
    /// ручное управление паузами внутри ReceiveAsync.
    /// </summary>
    public bool PauseOnBackpressure { get; set; } = true;

    /// <summary>Сколько сообщений внутренний буфер консьюмера готов держать до паузы партиций.</summary>
    public int BackpressureThreshold { get; set; } = 1024;

    /// <summary>Размер пакета fetch-а консьюмера (сообщений).</summary>
    public int FetchMaxBytes { get; set; } = 50 * 1024 * 1024;

    /// <summary>Таймаут session.timeout.ms.</summary>
    public int SessionTimeoutMs { get; set; } = 10_000;

    /// <summary>Произвольные свойства librdkafka поверх дефолтов.</summary>
    public Dictionary<string, string> AdditionalProperties { get; set; } = new(StringComparer.Ordinal);
}
