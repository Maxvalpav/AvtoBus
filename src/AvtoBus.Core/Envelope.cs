using System.Collections.Frozen;

namespace AvtoBus;

/// <summary>
/// Конверт сообщения — единственное, что реально путешествует между процессами.
/// Тело хранится как <see cref="ReadOnlyMemory{T}"/>: от транспорта до десериализатора копий нет.
/// </summary>
public sealed record Envelope
{
    public required Guid MessageId { get; init; }

    /// <summary>Идентификатор бизнес-потока: наследуется всеми каскадными сообщениями.</summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>MessageId сообщения-родителя — даёт полное дерево причинности.</summary>
    public Guid? CausationId { get; init; }

    /// <summary>Стабильное имя контракта, например <c>orders.order-placed.v1</c>.</summary>
    public required string MessageType { get; init; }

    public required ReadOnlyMemory<byte> Body { get; init; }

    public string ContentType { get; init; } = "application/json";

    public DateTimeOffset SentAt { get; init; }

    /// <summary>Отложенная доставка: сообщение не должно попасть консьюмеру раньше указанного момента.</summary>
    public DateTimeOffset? DeliverAt { get; init; }

    public TimeSpan? TimeToLive { get; init; }

    /// <summary>Ключ упорядочивания: Kafka key, ASB session, consistent-hash exchange в RabbitMQ.</summary>
    public string? PartitionKey { get; init; }

    public string? TenantId { get; init; }

    /// <summary>Приоритет 0-10 для priority queue + WFQ (SQS FIFO). 0 = обычный.</summary>
    public int Priority { get; init; }

    /// <summary>Адрес для ответа в паттерне request/response.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Номер попытки доставки, начиная с 1.</summary>
    public int DeliveryAttempt { get; init; } = 1;

    /// <summary>W3C Trace Context.</summary>
    public string? TraceParent { get; init; }

    public IReadOnlyDictionary<string, string> Headers { get; init; } = FrozenDictionary<string, string>.Empty;

    public bool IsExpired(DateTimeOffset now) => TimeToLive is { } ttl && now - SentAt >= ttl;

    public bool IsDue(DateTimeOffset now) => DeliverAt is not { } at || at <= now;

    public string? Header(string name) => Headers.TryGetValue(name, out var value) ? value : null;

    public Envelope WithHeader(string name, string value)
    {
        var headers = new Dictionary<string, string>(Headers, StringComparer.Ordinal) { [name] = value };
        return this with { Headers = headers.ToFrozenDictionary(StringComparer.Ordinal) };
    }
    public Envelope WithHeaders(IEnumerable<KeyValuePair<string, string>> headers)
    {
        var d = new Dictionary<string, string>(Headers, StringComparer.Ordinal);
        foreach (var kv in headers) d[kv.Key] = kv.Value;
        return this with { Headers = d.ToFrozenDictionary(StringComparer.Ordinal) };
    }

    public Envelope NextAttempt() => this with { DeliveryAttempt = DeliveryAttempt + 1 };
}

/// <summary>
/// Аудит «кто послал» (идея 332): текущий инициатор (user id/service account) бежит через AsyncLocal
/// и автоматически записывается в заголовок конверта <see cref="BusHeaders.Initiator"/>.
/// Приложение ставит его из HttpContext при обработке запроса — каскады шины наследуют автоматически.
/// </summary>
public static class InitiatorContext
{
    private static readonly AsyncLocal<Stack<string?>?> StackHolder = new();

    public static IDisposable Push(string? initiator)
    {
        var prev = StackHolder.Value;
        var next = prev is null ? new Stack<string?>() : new Stack<string?>(prev.Reverse());
        next.Push(initiator);
        StackHolder.Value = next;
        return new PopOnDispose(prev);
    }

    public static string? Get() => StackHolder.Value is { Count: > 0 } st ? st.Peek() : null;

    private sealed class PopOnDispose : IDisposable
    {
        private readonly Stack<string?>? _previous;
        private bool _disposed;
        public PopOnDispose(Stack<string?>? previous) => _previous = previous;
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            StackHolder.Value = _previous;
        }
    }
}

/// <summary>
/// Текущий тенант процесса отправки (идея 461): AsyncLocal-контекст, которым приложение помечает
/// сообщение. Приложение ставит его из HttpContext (аналог <see cref="InitiatorContext"/>),
/// EnvelopeFactory проставляет его в конверт, если тенант не задан явно.
/// </summary>
public static class TenantContext
{
    private static readonly AsyncLocal<Stack<string?>?> StackHolder = new();
    /// <summary>Устанавливает текущего тенанта (например, из middleware ASP.NET по claim/header).</summary>
    public static IDisposable Push(string? tenantId)
    {
        var prev = StackHolder.Value;
        var next = prev is null ? new Stack<string?>() : new Stack<string?>(prev.Reverse());
        next.Push(tenantId);
        StackHolder.Value = next;
        return new PopOnDispose(prev);
    }
    public static string? Get() => StackHolder.Value is { Count: > 0 } st ? st.Peek() : null;
    private sealed class PopOnDispose : IDisposable
    {
        private readonly Stack<string?>? _previous;
        private bool _disposed;
        public PopOnDispose(Stack<string?>? previous) => _previous = previous;
        public void Dispose() { if (_disposed) return; _disposed = true; StackHolder.Value = _previous; }
    }
}

/// <summary>Имена заголовков-конвенций. Никаких магических строк в пользовательском коде (идея 127).</summary>
public static class BusHeaders
{
    public const string IdempotencyKey = "avtobus-idempotency-key";
    public const string Source = "avtobus-source";
    public const string Initiator = "avtobus-initiator";
    public const string ContentEncoding = "content-encoding";
    public const string SchemaId = "avtobus-schema-id";
    public const string ExceptionType = "avtobus-exception-type";
    public const string ExceptionMessage = "avtobus-exception-message";
    public const string ExceptionStackTrace = "avtobus-exception-stack";
    public const string FailedQueue = "avtobus-failed-queue";
    public const string FailedAt = "avtobus-failed-at";
    public const string DeadLetterReason = "avtobus-dead-letter-reason";
    public const string OriginalDestination = "avtobus-original-destination";
    public const string Sequence = "avtobus-sequence";

    /// <summary>Сколько hops прошло сообщение: защита от раздувания контекста (идея 313).</summary>
    public const string Hops = "avtobus-hops";

    /// <summary>Подписанный контекст пользователя: сериализованный ClaimsPrincipal (идея 454).</summary>
    public const string User = "avtobus-user";

    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";
}

/// <summary>
/// Точка интеграции подсистемы безопасности на границе конверта (идея 451): подпись/шифрование
/// применяются к исходящему конверту до транспорта, а к входящему — до десериализации тела.
/// Реализуется в <c>AvtoBus.Security</c>; ядро лишь вызывает его, если оно подключено.
/// </summary>
public interface IEnvelopeSecurity
{
    /// <summary>Защищает исходящий конверт: подпись (и, если включено, шифрование тела).</summary>
    Envelope ProtectOutbound(Envelope envelope, string? serviceIdentity);

    /// <summary>
    /// Проверяет и открывает входящий конверт. Выбрасывает <see cref="SecurityViolationException"/>
    /// при неверной подписи или повреждённом шифровании — ядро отправляет такое сообщение в DLQ.
    /// </summary>
    Envelope OpenInbound(Envelope envelope);

    /// <summary>Включён ли хотя бы один механизм.</summary>
    bool IsEnabled { get; }
}

/// <summary>Сбой проверки безопасности входящего сообщения (идея 451).</summary>
public sealed class SecurityViolationException(string reason, Exception? inner = null)
    : Exception(reason, inner);

/// <summary>
/// Политика маршрутизации по регионам (идея 467): «данные не должны покинуть свой регион».
/// Вызывается ядром на исходящем пути до отправки в транспорт. Реализуется в <c>AvtoBus.Multitenancy</c>;
/// ядро лишь проверяет его, если подключено.
/// </summary>
public interface IRegionPolicy
{
    /// <summary>
    /// Проверяет, можно ли отправить конверт в указанное назначение. Выбрасывает
    /// <see cref="RegionViolationException"/>, если data-residency запрещает маршрут.
    /// </summary>
    void Validate(Envelope envelope, TransportDestination destination);
}

/// <summary>Попытка отправить сообщение в регион, которому его данные не принадлежат (идея 467).</summary>
public sealed class RegionViolationException(string reason, Exception? inner = null)
    : Exception(reason, inner);

/// <summary>
/// Политика изоляции тенантов на уровне хранилища (идея 462, уровни B/C):
/// переписывает destination так, чтобы тенанты физически не делили очередь/неймспейс.
/// Вызывается ядром на исходящем пути (до отправки в транспорт) и на входящем
/// (расширение подписок консьюмеров на изолированные очереди). Реализуется в
/// <c>AvtoBus.Multitenancy</c>; ядро лишь вызывает его, если подключено.
/// </summary>
public interface ITenantIsolationPolicy
{
    /// <summary>
    /// Возвращает назначение, изолированное для указанного тенанта. Уровень Shared —
    /// возвращает исходное назначение без изменений (тенант живёт в общей очереди).
    /// </summary>
    TransportDestination Isolate(TransportDestination destination, string tenantId);

    /// <summary>Все зарегистрированные тенанты — для расширения подписок на их очереди.</summary>
    IReadOnlyCollection<string> TenantIds { get; }
}
