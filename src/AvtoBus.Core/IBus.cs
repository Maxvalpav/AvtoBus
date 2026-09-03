namespace AvtoBus;

/// <summary>
/// Главная точка входа: публикация событий, отправка команд, запрос-ответ, отложенная доставка.
/// </summary>
public interface IBus
{
    /// <summary>Событие уходит всем подписчикам (0..N).</summary>
    ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default)
        where T : class;

    /// <summary>Команда уходит ровно одному владельцу очереди.</summary>
    ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default)
        where T : class;

    /// <summary>Запрос с ожиданием типизированного ответа.</summary>
    ValueTask<TReply> RequestAsync<TRequest, TReply>(
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
        where TRequest : class
        where TReply : class;

    /// <summary>Отложенная доставка. Возвращает токен для отмены через <see cref="CancelScheduledAsync"/> (идея 46).</summary>
    ValueTask<ScheduledToken> ScheduleAsync<T>(T message, DateTimeOffset at, CancellationToken ct = default)
        where T : class;

    ValueTask CancelScheduledAsync(ScheduledToken token, CancellationToken ct = default);
}

/// <summary>Идентификатор отложенного сообщения; позволяет отменить доставку до срабатывания.</summary>
public readonly record struct ScheduledToken(Guid Value)
{
    public override string ToString() => Value.ToString();
}

/// <summary>Общая часть опций отправки: то, что кладётся в конверт.</summary>
public abstract class MessageOptions
{
    internal Dictionary<string, string>? HeaderBag;

    public Guid? MessageId { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
    public string? PartitionKey { get; set; }
    public string? TenantId { get; set; }
    public TimeSpan? TimeToLive { get; set; }
    public DateTimeOffset? DeliverAt { get; set; }

    /// <summary>Приоритет очереди с поддержкой приоритетов (0-10, 10 = максимум). WFQ учитывает вес тенанта.</summary>
    public int Priority { get; set; }

    /// <summary>Явное назначение вместо вычисленного по правилам маршрутизации.</summary>
    public string? Destination { get; set; }

    /// <summary>Имя транспорта для мульти-транспортных конфигураций (идея 73).</summary>
    public string? Transport { get; set; }

    public IReadOnlyDictionary<string, string> Headers
        => HeaderBag ?? (IReadOnlyDictionary<string, string>)EmptyHeaders;

    private static readonly Dictionary<string, string> EmptyHeaders = new(0);

    public MessageOptions WithHeader(string name, string value)
    {
        (HeaderBag ??= new Dictionary<string, string>(StringComparer.Ordinal))[name] = value;
        return this;
    }

    public MessageOptions WithCorrelation(Guid correlationId)
    {
        CorrelationId = correlationId;
        return this;
    }

    public MessageOptions WithPartitionKey(string key)
    {
        PartitionKey = key;
        return this;
    }

    /// <summary>Доставить не раньше, чем через указанный интервал (идея 18).</summary>
    public MessageOptions WithDelay(TimeSpan delay)
    {
        DeliverAt = DateTimeOffset.UtcNow + delay;
        return this;
    }

    public MessageOptions WithTimeToLive(TimeSpan ttl)
    {
        TimeToLive = ttl;
        return this;
    }

    public MessageOptions WithPriority(int priority)
    {
        if (priority is < 0 or > 10) throw new ArgumentOutOfRangeException(nameof(priority), "Priority 0..10");
        Priority = priority;
        WithHeader("avtobus.priority", priority.ToString());
        return this;
    }
}

public sealed class PublishOptions : MessageOptions;

public sealed class SendOptions : MessageOptions
{
    /// <summary>Адрес для ответа; заполняется автоматически в request/response.</summary>
    public string? ReplyTo { get; set; }
}
