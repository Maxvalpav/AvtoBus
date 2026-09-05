namespace AvtoBus.Redis;

/// <summary>Настройки Redis-стора inbox-дедупликации (04 §1.2).</summary>
public sealed class RedisInboxOptions
{
    /// <summary>Строка подключения: <c>localhost:6379</c>, опционально с паролем.</summary>
    public string Configuration { get; set; } = "localhost:6379";

    /// <summary>Окно дедупликации (TTL ключа <c>SET NX EX</c>). Дефолт 24ч = прод-пресет.</summary>
    public TimeSpan Window { get; set; } = TimeSpan.FromHours(24);
}
