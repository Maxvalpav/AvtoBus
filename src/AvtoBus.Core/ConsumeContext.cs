using System.Security.Claims;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus;

/// <summary>
/// Всё, что нужно хендлеру и middleware в рамках обработки одного сообщения.
/// Аналог <c>HttpContext</c>: живёт ровно один consume, скоуп DI — свой.
/// </summary>
public class ConsumeContext
{
    private Dictionary<object, object?>? _items;
    private List<OutgoingMessage>? _outgoing;

    internal ConsumeContext(
        Envelope envelope,
        object message,
        IServiceProvider services,
        IBus bus,
        CancellationToken cancellationToken)
    {
        Envelope = envelope;
        Message = message;
        Services = services;
        Bus = bus;
        CancellationToken = cancellationToken;
    }

    public Envelope Envelope { get; private set; }

    public object Message { get; private set; }

    internal void ReplaceEnvelope(Envelope envelope, object? message = null)
    {
        Envelope = envelope;
        if (message is not null) Message = message;
    }

    /// <summary>Scoped-провайдер: один скоуп на сообщение (идея 14).</summary>
    public IServiceProvider Services { get; }

    public IBus Bus { get; }

    public CancellationToken CancellationToken { get; internal set; }

    /// <summary>Номер попытки, начиная с 1.</summary>
    public int Attempt => Envelope.DeliveryAttempt;

    /// <summary>Очередь/топик, из которого пришло сообщение.</summary>
    public required TransportDestination Source { get; init; }

    /// <summary>Обмен данными между middleware в рамках одной обработки (идея 13).</summary>
    public IDictionary<object, object?> Items => _items ??= new Dictionary<object, object?>();

    /// <summary>Исход обработки: заполняется пайплайном, читается транспортным слоем.</summary>
    public ConsumeOutcome Outcome { get; internal set; } = ConsumeOutcome.Handled;

    public string? DeadLetterReason { get; private set; }

    /// <summary>Явно запрошенная задержка перед следующей попыткой.</summary>
    public TimeSpan? DeferralDelay { get; private set; }

    /// <summary>
    /// Исходящие сообщения, накопленные хендлером. Отправляются пайплайном ПОСЛЕ успешной обработки —
    /// это и есть «каскады через outbox»: упал хендлер — ничего не улетело.
    /// </summary>
    public IReadOnlyList<OutgoingMessage> Outgoing
        => (IReadOnlyList<OutgoingMessage>?)_outgoing ?? Array.Empty<OutgoingMessage>();

    /// <summary>Опубликовать событие как каскад текущего сообщения.</summary>
    public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null) where T : class
    {
        Enqueue(new OutgoingMessage(@event, OutgoingKind.Publish, options));
        return ValueTask.CompletedTask;
    }

    /// <summary>Отправить команду как каскад текущего сообщения.</summary>
    public ValueTask SendAsync<T>(T command, SendOptions? options = null) where T : class
    {
        Enqueue(new OutgoingMessage(command, OutgoingKind.Send, options));
        return ValueTask.CompletedTask;
    }

    /// <summary>Отложенная отправка как каскад.</summary>
    public ValueTask ScheduleAsync<T>(T message, TimeSpan delay) where T : class
    {
        // Используем системное время для ScheduleAsync каскада — TimeProvider доступен via EnvelopeFactory для точного времени создания;
        // здесь UtcNow достаточен т.к. задержка относительная, но можно было бы инжектить TimeProvider.
        var options = new SendOptions { DeliverAt = DateTimeOffset.UtcNow + delay };
        Enqueue(new OutgoingMessage(message, OutgoingKind.Send, options));
        return ValueTask.CompletedTask;
    }

    /// <summary>Ответить на request. Уходит в <see cref="Envelope.ReplyTo"/>.</summary>
    public ValueTask RespondAsync<T>(T reply) where T : class
    {
        if (Envelope.ReplyTo is null)
            throw new InvalidOperationException(
                $"Сообщение '{Envelope.MessageType}' пришло без ReplyTo — отвечать некуда. " +
                "RespondAsync применим только к сообщениям, отправленным через RequestAsync.");

        Enqueue(new OutgoingMessage(reply, OutgoingKind.Respond, null));
        return ValueTask.CompletedTask;
    }

    /// <summary>Отложить повтор на указанный интервал; сообщение вернётся в очередь (идея 34).</summary>
    public ValueTask DeferAsync(TimeSpan delay)
    {
        DeferralDelay = delay;
        Outcome = ConsumeOutcome.Deferred;
        return ValueTask.CompletedTask;
    }

    /// <summary>Отправить в DLQ без ретраев: сообщение обработать невозможно в принципе.</summary>
    public void DeadLetter(string reason)
    {
        DeadLetterReason = reason;
        Outcome = ConsumeOutcome.DeadLettered;
    }

    /// <summary>Сообщение осознанно пропущено (дубликат, устаревшее состояние) — не ошибка (идея 199).</summary>
    public void Skip(string? reason = null)
    {
        DeadLetterReason = reason;
        Outcome = ConsumeOutcome.Skipped;
    }

    /// <summary>Пришло более новое состояние, это — уже неактуально (идея 199).</summary>
    public void Superseded()
    {
        Outcome = ConsumeOutcome.Superseded;
    }

    public T Resolve<T>() where T : notnull => Services.GetRequiredService<T>();

    /// <summary>
    /// Пользователь, от чьего имени обрабатывается сообщение (идея 454).
    /// Восстанавливается из подписанного контекста в заголовках конверта middleware'ом авторизации.
    /// </summary>
    public ClaimsPrincipal? Principal { get; internal set; }

    internal void Enqueue(OutgoingMessage message) => (_outgoing ??= []).Add(message);

    internal void ClearOutgoing() => _outgoing?.Clear();
}

/// <summary>Типизированный контекст: <c>ctx.Message</c> уже нужного типа.</summary>
public sealed class ConsumeContext<T> : ConsumeContext where T : class
{
    internal ConsumeContext(
        Envelope envelope,
        T message,
        IServiceProvider services,
        IBus bus,
        CancellationToken cancellationToken)
        : base(envelope, message, services, bus, cancellationToken)
    {
        Message = message;
    }

    public new T Message { get; }
}

/// <summary>Семантический исход обработки (идея 199).</summary>
public enum ConsumeOutcome
{
    /// <summary>Обработано успешно.</summary>
    Handled,

    /// <summary>Осознанно пропущено: дубликат, фильтр, неприменимо.</summary>
    Skipped,

    /// <summary>Устарело: пришло более новое состояние.</summary>
    Superseded,

    /// <summary>Отложено на повтор по запросу хендлера.</summary>
    Deferred,

    /// <summary>Обработать невозможно — сразу в DLQ, без ретраев.</summary>
    DeadLettered,

    /// <summary>Упало исключением; судьбу решает recoverability.</summary>
    Failed,
}
