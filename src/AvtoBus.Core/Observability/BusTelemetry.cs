using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace AvtoBus.Observability;

/// <summary>
/// Трейсы и метрики по семантическим конвенциям OpenTelemetry для messaging (идеи 301, 302).
/// Ноль конфигурации: достаточно подписаться на источник в OTel SDK.
/// </summary>
public static class BusTelemetry
{
    public const string ActivitySourceName = "AvtoBus";
    public const string MeterName = "AvtoBus";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, "0.1.0");

    private static readonly Meter Meter = new(MeterName, "0.1.0");

    /// <summary>Длительность обработки сообщения консьюмером.</summary>
    public static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>(
        "avtobus.consume.duration",
        unit: "ms",
        description: "Время обработки сообщения хендлером");

    /// <summary>Полное время от отправки до завершения обработки — главный SLA-показатель (идея 303).</summary>
    public static readonly Histogram<double> CriticalTime = Meter.CreateHistogram<double>(
        "avtobus.critical.time",
        unit: "ms",
        description: "Время от отправки до окончания обработки, включая ожидание в очереди");

    /// <summary>RTT канарейки: publish → транспорт → consume — живой end-to-end healthcheck (идея 337).</summary>
    public static readonly Histogram<double> CanaryRtt = Meter.CreateHistogram<double>(
        "avtobus.canary.rtt",
        unit: "ms",
        description: "Полное время цикла системной канарейки через всю цепочку");

    /// <summary>Время каждого шага пайплайна — «водопад» обработки показывает, что тормозит (идея 334).</summary>
    public static readonly Histogram<double> PipelineStepDuration = Meter.CreateHistogram<double>(
        "avtobus.pipeline.step.duration",
        unit: "ms",
        description: "Длительность одного middleware-шага обработки сообщения");

    /// <summary>Размер полезной нагрузки при отправке — для контроля врапперов и сжатия.</summary>
    public static readonly Histogram<double> PublishBytes = Meter.CreateHistogram<double>(
        "avtobus.publish.bytes",
        unit: "bytes",
        description: "Размер тела опубликованного сообщения");

    /// <summary>Размер полезной нагрузки при обработке.</summary>
    public static readonly Histogram<double> ConsumeBytes = Meter.CreateHistogram<double>(
        "avtobus.consume.bytes",
        unit: "bytes",
        description: "Размер тела обработанного сообщения");

    public static readonly Counter<long> PublishCount = Meter.CreateCounter<long>(
        "avtobus.publish.count",
        description: "Количество опубликованных сообщений");

    public static readonly Counter<long> ConsumeCount = Meter.CreateCounter<long>(
        "avtobus.consume.count",
        description: "Количество обработанных сообщений");

    public static readonly Counter<long> RetryCount = Meter.CreateCounter<long>(
        "avtobus.retry.count",
        description: "Количество повторных попыток обработки");

    public static readonly Counter<long> DeadLetterCount = Meter.CreateCounter<long>(
        "avtobus.deadletter.count",
        description: "Количество сообщений, отправленных в DLQ");

    /// <summary>Ответы, пришедшие после таймаута/отмены запроса — подтверждены, не ретраятся (идея 48).</summary>
    public static readonly Counter<long> LateReplyCount = Meter.CreateCounter<long>(
        "avtobus.reply.late",
        description: "Ответы на истёкшие запросы: ack, без повторной доставки и без диспетчеризации");

    public static readonly Counter<long> HeaderTruncationCount = Meter.CreateCounter<long>(
        "avtobus.headers.truncated",
        description: "Сколько раз контекст (header-ы) сообщения был обрезан по лимитам (идея 313)");

    public static readonly Counter<long> BlacklistBlockedCount = Meter.CreateCounter<long>(
        "avtobus.blacklist.blocked",
        description: "Сообщения, отклонённые чёрным списком на лету (идея 349)");

    /// <summary>Срабатывания cron-джобов (идея 223).</summary>
    public static readonly Counter<long> CronFiredCount = Meter.CreateCounter<long>(
        "avtobus.cron.fired",
        description: "Количество срабатываний cron-расписаний");

    /// <summary>Сообщения, отклонённые подсистемой безопасности (плохая подпись, шифрование) (идея 451).</summary>
    public static readonly Counter<long> SecurityViolationCount = Meter.CreateCounter<long>(
        "avtobus.security.violations",
        description: "Входящие сообщения, отклонённые подсистемой безопасности");

    /// <summary>Доставленные отложенные сообщения (идея 226).</summary>
    public static readonly Counter<long> ScheduledDeliveredCount = Meter.CreateCounter<long>(
        "avtobus.scheduled.delivered",
        description: "Количество доставленных из durable-хранилища отложенных сообщений");

    /// <summary>Отставание async-проекции от головы event store — main SLA read-моделей (идея 251).</summary>
    public static readonly Histogram<long> ProjectionLag = Meter.CreateHistogram<long>(
        "avtobus.projection.lag",
        unit: "events",
        description: "Отставание проекции от головы глобального потока событий");

    /// <summary>
    /// Открывает спан отправки. Имя по конвенции OTel: <c>{destination} publish</c>.
    /// </summary>
    public static Activity? StartPublish(string messageType, TransportDestination destination)
    {
        var activity = ActivitySource.StartActivity($"{destination.Name} publish", ActivityKind.Producer);
        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "avtobus");
        activity.SetTag("messaging.operation", "publish");
        activity.SetTag("messaging.destination.name", destination.Name);
        activity.SetTag("messaging.destination.kind", destination.Kind.ToString().ToLowerInvariant());
        activity.SetTag("messaging.message.type", messageType);
        return activity;
    }

    /// <summary>Диагностическое событие: сообщение опубликовано (идея 331).</summary>
    public static void PublishRecorded(string messageType, TransportDestination destination, int bytes)
        => AvtoBusEventSource.Log.MessagePublished(messageType, destination.Name, bytes);

    /// <summary>Диагностическое событие: сообщение обработано (идея 331).</summary>
    public static void ConsumeRecorded(string messageType, TransportDestination source, long durationMs)
        => AvtoBusEventSource.Log.MessageConsumed(messageType, source.Name, durationMs);

    /// <summary>Диагностическое событие: обработка упала с исключением (идея 331).</summary>
    public static void FailureRecorded(Envelope envelope, int attempt, Exception exception)
        => AvtoBusEventSource.Log.MessageFailed(
            envelope.MessageType,
            envelope.MessageId.ToString("N"),
            attempt,
            exception.GetType().FullName ?? exception.GetType().Name);

    /// <summary>Канарейка долетела: RTT в histogram + событие (идея 337).</summary>
    public static void CanaryCompleted(double rttMs)
    {
        CanaryRtt.Record(rttMs, new KeyValuePair<string, object?>("messaging.operation", "canary"));
        AvtoBusEventSource.Log.CanaryCompleted(rttMs);
    }

    /// <summary>Канарейка потерялась: фиксируем таймаут и шлём диагностическое событие.</summary>
    public static void CanaryFailed(string serviceName)
        => AvtoBusEventSource.Log.CanaryLost(serviceName);

    /// <summary>Канарейка не вернулась в срок — Recording опасного замедления.</summary>
    public static void CanaryTimeout(string serviceName, TimeSpan elapsed)
        => AvtoBusEventSource.Log.CanaryLost($"{serviceName} ({elapsed.TotalSeconds:0.##}s)");

    /// <summary>Открывает спан обработки, связывая его с трейсом отправителя через traceparent.</summary>
    public static Activity? StartConsume(Envelope envelope, TransportDestination source, string handlerName)
    {
        ActivityContext.TryParse(envelope.TraceParent, null, out var parent);

        var activity = ActivitySource.StartActivity(
            $"{source.Name} process",
            ActivityKind.Consumer,
            parent);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "avtobus");
        activity.SetTag("messaging.operation", "process");
        activity.SetTag("messaging.destination.name", source.Name);
        activity.SetTag("messaging.message.type", envelope.MessageType);
        activity.SetTag("messaging.message.id", envelope.MessageId);
        activity.SetTag("messaging.avtobus.handler", handlerName);
        activity.SetTag("messaging.avtobus.attempt", envelope.DeliveryAttempt);

        if (envelope.CorrelationId is { } correlation)
            activity.SetTag("messaging.message.conversation_id", correlation);

        if (envelope.TenantId is { } tenant)
            activity.SetTag("messaging.avtobus.tenant", tenant);

        return activity;
    }

    /// <summary>Записывает решение recoverability как событие спана — вся судьба сообщения в трейсе (идея 195).</summary>
    public static void RecordDecision(Activity? activity, string decision, string reason)
    {
        activity?.AddEvent(new ActivityEvent(
            "avtobus.recoverability",
            tags: new ActivityTagsCollection
            {
                ["decision"] = decision,
                ["reason"] = reason,
            }));

        if (activity?.GetTagItem("messaging.message.type") is string messageType
            && activity.GetTagItem("messaging.message.id") is string messageId)
            AvtoBusEventSource.Log.DecisionMade(messageType, messageId, decision, reason);
    }

    /// <summary>Замер одного шага пайплайна: метка имени + длительность (идея 334).</summary>
    public static void RecordPipelineStep(string step, string messageType, double durationMs)
        => PipelineStepDuration.Record(
            durationMs,
            new KeyValuePair<string, object?>("messaging.avtobus.pipeline.step", step),
            new KeyValuePair<string, object?>("messaging.message.type", messageType));

    /// <summary>Контекст сообщения обрезан по лимитам: счётчик + диагностическое событие (идея 313).</summary>
    public static void HeaderTruncated(string messageType, string reason)
    {
        HeaderTruncationCount.Add(1, new KeyValuePair<string, object?>("messaging.message.type", messageType));
        AvtoBusEventSource.Log.ContextTruncated(messageType, reason);
    }

    /// <summary>Сообщение отклонено чёрным списком: счётчик + диагностическое событие (идея 349).</summary>
    public static void Blacklisted(string messageType, string messageId, string reason)
    {
        BlacklistBlockedCount.Add(1, new KeyValuePair<string, object?>("messaging.message.type", messageType));
        AvtoBusEventSource.Log.MessageBlacklisted(messageType, messageId, reason);
    }

    /// <summary>Сообщение отклонено безопасностью: счётчик + диагностическое событие (идея 451).</summary>
    public static void SecurityViolation(string reason, string messageType)
    {
        SecurityViolationCount.Add(1, new KeyValuePair<string, object?>("messaging.message.type", messageType));
        AvtoBusEventSource.Log.MessageSecurityViolation(messageType, reason);
    }

    /// <summary>Поздний ответ на истёкший запрос: счётчик (ack без requeue) (идея 48).</summary>
    public static void LateReply(string messageType)
        => LateReplyCount.Add(1, new KeyValuePair<string, object?>("messaging.message.type", messageType));
}
