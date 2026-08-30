using AvtoBus.Configuration;
using AvtoBus.Observability;

namespace AvtoBus.Runtime;

/// <summary>
/// Реализация <see cref="IBus"/>: маршрутизирует, сериализует и отдаёт транспорту.
/// Синглтон: состояния на сообщение не держит.
/// </summary>
public sealed class AvtoBusClient(
    BusOptions options,
    TransportRegistry transports,
    EnvelopeFactory envelopes,
    ReplyRouter replies,
    MessageRegistry registry,
    IUniqueStore? uniqueStore = null) : IBus
{
    public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default)
        where T : class
        => DispatchAsync(@event, typeof(T), OutgoingKind.Publish, options, parent: null, ct);

    public ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default)
        where T : class
        => DispatchAsync(command, typeof(T), OutgoingKind.Send, options, parent: null, ct);

    public async ValueTask<TReply> RequestAsync<TRequest, TReply>(
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
        where TRequest : class
        where TReply : class
    {
        var sendOptions = new SendOptions
        {
            MessageId = Guid.NewGuid(),
            ReplyTo = replies.ReplyAddress,
        };

        // Регистрируем ожидание ДО отправки: ответ может прилететь быстрее, чем вернётся Send.
        var waiting = replies.RegisterAsync(
            sendOptions.MessageId.Value,
            typeof(TReply),
            timeout ?? options.DefaultRequestTimeout,
            ct);

        await DispatchAsync(request, typeof(TRequest), OutgoingKind.Send, sendOptions, parent: null, ct)
            .ConfigureAwait(false);

        return (TReply)await waiting.ConfigureAwait(false);
    }

    public async ValueTask<ScheduledToken> ScheduleAsync<T>(T message, DateTimeOffset at, CancellationToken ct = default)
        where T : class
    {
        var sendOptions = new SendOptions { MessageId = Guid.NewGuid(), DeliverAt = at };
        await DispatchAsync(message, typeof(T), OutgoingKind.Send, sendOptions, parent: null, ct).ConfigureAwait(false);
        return new ScheduledToken(sendOptions.MessageId.Value);
    }

    public ValueTask CancelScheduledAsync(ScheduledToken token, CancellationToken ct = default)
    {
        // Отмена — свойство транспорта: не каждый брокер умеет снять уже принятое сообщение.
        foreach (var transport in transports.All)
        {
            if (transport is IScheduleCancellable cancellable)
                cancellable.CancelScheduled(token.Value);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Отправляет одно сообщение. Общий путь для publish/send/respond и для каскадов из хендлеров.
    /// </summary>
    internal async ValueTask DispatchAsync(
        object message,
        Type messageType,
        OutgoingKind kind,
        MessageOptions? messageOptions,
        Envelope? parent,
        CancellationToken ct)
    {
        // UniqueJob producer-side check (Oban/River/Sidekiq – Elixir/Go/Ruby):
        // не отправляем дубликат пока предыдущий ещё в окне уникальности.
        if (uniqueStore is not null && parent is null)
        {
            var explicitKey = messageOptions?.Headers.TryGetValue("avtobus.unique-key", out var k) == true ? k : null;
            if (explicitKey is not null)
            {
                var ttlSec = messageOptions!.Headers.TryGetValue("avtobus.unique-ttl", out var ttlStr) && double.TryParse(ttlStr, out var sec) ? TimeSpan.FromSeconds(sec) : TimeSpan.FromSeconds(30);
                if (!uniqueStore.TryAcquire(explicitKey, ttlSec))
                    return; // skip duplicate silently как Oban
            }
            else
            {
                var attr = messageType.GetCustomAttributes(typeof(UniqueJobAttribute), true).FirstOrDefault() as UniqueJobAttribute;
                if (attr is not null)
                {
                    var dest = ResolveDestination(messageType, kind, messageOptions, parent);
                    var key = UniqueKeyComputer.Compute(message, messageType, dest.Name ?? dest.ToString() ?? "", attr);
                    if (!uniqueStore.TryAcquire(key, attr.Period))
                    {
                        if (attr.OnConflict == UniqueConflictBehavior.Throw)
                            throw new DuplicateMessageException(key, messageType.Name);
                        return; // skip
                    }
                }
            }
        }

        var (envelope, destination, transportName, activity) = Prepare(message, messageType, kind, messageOptions, parent);
        using var _ = activity;

        var transport = transports.Get(messageOptions?.Transport ?? transportName);

        await transport.SendAsync(envelope, destination, ct).ConfigureAwait(false);

        BusTelemetry.PublishCount.Add(1,
            new KeyValuePair<string, object?>("messaging.message.type", envelope.MessageType),
            new KeyValuePair<string, object?>("messaging.destination.name", destination.Name));
        BusTelemetry.PublishBytes.Record(envelope.Body.Length,
            new KeyValuePair<string, object?>("messaging.message.type", envelope.MessageType),
            new KeyValuePair<string, object?>("messaging.destination.name", destination.Name));

        BusTelemetry.PublishRecorded(envelope.MessageType, destination, envelope.Body.Length);
    }

    /// <summary>
    /// Строит конверт и резолвит назначение без фактической отправки: общий путь для
    /// немедленной отправки в транспорт и для записи в транзакционный outbox (IMessageSession).
    /// Publish-активность стартует ДО создания конверта: <see cref="Envelope.TraceParent"/>
    /// должен указывать на publish-спан, чтобы consume-спан был его дочерним (трейс publish → consume).
    /// Возвращённую активность вызывающий обязан остановить (using).
    /// </summary>
    internal (Envelope Envelope, TransportDestination Destination, string? TransportName, System.Diagnostics.Activity? PublishActivity) Prepare(
        object message,
        Type messageType,
        OutgoingKind kind,
        MessageOptions? messageOptions,
        Envelope? parent)
    {
        var destination = ResolveDestination(messageType, kind, messageOptions, parent);

        var activity = BusTelemetry.StartPublish(registry.NameOf(messageType), destination);

        var envelope = envelopes.Create(message, messageType, messageOptions, parent);

        // Data-residency: запрещённый маршрут между регионами блокируется ДО отправки в транспорт (идея 467).
        options.RegionPolicy?.Validate(envelope, destination);

        // Изоляция тенантов на уровне хранилища (идея 462, уровень B/C): destination
        // переписывается так, чтобы тенант физически не делил очередь/неймспейс с другими.
        if (options.TenantIsolationPolicy is { } isolation && envelope.TenantId is { } tenantId)
            destination = isolation.Isolate(destination, tenantId);

        // Ответ адресуется конкретному запросу: получатель найдёт ожидающего по CausationId.
        if (kind is OutgoingKind.Respond && parent is not null)
            envelope = envelope with { CausationId = parent.MessageId, CorrelationId = parent.CorrelationId };

        return (envelope, destination, ResolveTransportName(messageType, kind), activity);
    }

    private TransportDestination ResolveDestination(
        Type messageType,
        OutgoingKind kind,
        MessageOptions? messageOptions,
        Envelope? parent)
    {
        // Ответ всегда идёт в reply-очередь запроса, никакие правила маршрутизации не применяются.
        if (kind is OutgoingKind.Respond)
        {
            var replyTo = parent?.ReplyTo
                          ?? throw new InvalidOperationException("Ответ невозможен: у запроса не задан ReplyTo.");
            return TransportDestination.Queue(replyTo);
        }

        if (messageOptions?.Destination is { } explicitName)
            return kind is OutgoingKind.Send
                ? TransportDestination.Queue(explicitName)
                : TransportDestination.Topic(explicitName);

        return options.Routing.Resolve(messageType, kind).Destination is { Name: not null } routed
            ? routed
            : RoutingTable.Conventional(messageType, kind);
    }

    private string? ResolveTransportName(Type messageType, OutgoingKind kind)
        => options.Routing.Resolve(messageType, kind).Transport;
}

/// <summary>Транспорт, умеющий снять ещё не доставленное отложенное сообщение (идея 46).</summary>
public interface IScheduleCancellable
{
    bool CancelScheduled(Guid messageId);
}
