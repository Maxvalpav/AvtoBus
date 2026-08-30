namespace AvtoBus.Redis;

/// <summary>
/// Настройки Redis Streams-транспорта (идея 65): consumer groups, XAUTOCLAIM
/// для переподхвата зависших сообщений упавших консьюмеров, батчи.
/// </summary>
public sealed class RedisOptions
{
    /// <summary>Строка подключения: <c>localhost:6379</c>, опционально с паролем.</summary>
    public string Configuration { get; set; } = "localhost:6379";

    /// <summary>Имя приложения — суффикс имён консьюмеров и групп.</summary>
    public string Name { get; set; } = "avtobus";

    /// <summary>Размер batch-а чтения из стрима за один XREADGROUP.</summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>Сколько времени консьюмер ждёт сообщения в блокирующем чтении.</summary>
    public TimeSpan BlockTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Минимальный idle-возраст pending-сообщения для XAUTOCLAIM (мс): сообщение считается
    /// зависшим, если консьюмер не ack-нул его дольше этого времени (идея 65).
    /// </summary>
    public long MinIdleTimeMs { get; set; } = 30_000;

    /// <summary>Интервал фонового XAUTOCLAIM-переподхвата зависших сообщений.</summary>
    public TimeSpan ReclaimInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Максимальная длина стрима (примерный обрез по XTRIM, 0 — без лимита).</summary>
    public int MaxStreamLength { get; set; } = 1_000_000;
}
