using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using AvtoBus.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Runtime;

/// <summary>
/// Обрабатывает одно сообщение: десериализация → пайплайн → хендлеры → каскады → решение о судьбе.
/// Здесь живёт вся политика надёжности; транспорт остаётся тупым каналом.
/// </summary>
public sealed class MessageProcessor(
    BusOptions options,
    DispatcherRegistry dispatchers,
    FailedConsumerRegistry failedConsumers,
    MessageRegistry registry,
    IServiceProvider rootServices,
    AvtoBusClient bus,
    ReplyRouter replies,
    BusDelegate pipeline,
    TimeProvider time,
    ILogger<MessageProcessor> logger)
{
    private readonly InboxDeduplication? _inbox = options.InboxWindow is { } window
        ? new InboxDeduplication(window, time)
        : null;

    private readonly RetryBudget _retryBudget = new(
        options.Recoverability.RetryBudget,
        window: TimeSpan.FromSeconds(10),
        time);

    private readonly Random _random = new();

    /// <summary>
    /// Полный цикл обработки: возвращает решение, что сделать с сообщением на уровне транспорта.
    /// Исключения наружу не выпускает — иначе цикл консьюмера умрёт на первом же баге в хендлере.
    /// </summary>
    public async ValueTask<ProcessingDecision> ProcessAsync(
        ITransportMessage transportMessage,
        TransportDestination source,
        CancellationToken ct)
    {
        var envelope = transportMessage.Envelope;

        using var activity = BusTelemetry.StartConsume(envelope, source, "message-processor");

        // 0. Подключенная безопасность проверяет подпись и/или открывает шифрованное тело —
        // до десериализации. Сбой — poison, ретраи тут бесполезны (идея 451).
        if (options.EnvelopeSecurity is { } security)
        {
            try
            {
                envelope = security.OpenInbound(envelope);
            }
            catch (SecurityViolationException exception)
            {
                BusTelemetry.SecurityViolation(exception.Message, envelope.MessageType);
                logger.LogWarning(
                    "Сообщение {MessageType} ({MessageId}) отклонено безопасностью в {Source}: {Reason}",
                    envelope.MessageType,
                    envelope.MessageId,
                    source.Name,
                    exception.Message);
                return ProcessingDecision.Poison(exception.Message);
            }
        }

        // 1. Разрешаем тип. Неизвестный контракт — это poison: ретраи не помогут.
        if (!registry.TryResolve(envelope.MessageType, out var messageType))
        {
            BusTelemetry.RecordDecision(Activity.Current, "poison", $"неизвестный тип контракта '{envelope.MessageType}'");
            logger.LogWarning(
                "Неизвестный тип сообщения {MessageType} ({MessageId}) — в poison-очередь",
                envelope.MessageType,
                envelope.MessageId);

            return ProcessingDecision.Poison($"неизвестный тип контракта '{envelope.MessageType}'");
        }

        // 2. Ответ на request/response завершает ожидание локально, минуя хендлеры.
        if (envelope.CausationId is { } requestId)
        {
            if (replies.IsAwaiting(requestId))
            {
                var reply = Deserialize(envelope, messageType);
                if (reply is not null && replies.TryComplete(requestId, reply))
                    return ProcessingDecision.Ack;
            }
            else if (source.Name == replies.ReplyAddress)
            {
                // Ответ пришёл после таймаута/отмены запроса: подтверждаем, не ретраим
                // и не диспатчим в хендлеры — ожидающего больше нет (идея 48).
                BusTelemetry.LateReply(envelope.MessageType);
                logger.LogDebug(
                    "Поздний ответ {MessageId} ({MessageType}) — ожидание истекло, подтверждён",
                    envelope.MessageId,
                    envelope.MessageType);
                return ProcessingDecision.Ack;
            }
        }

        var handlers = dispatchers.For(messageType);
        if (handlers.Length == 0)
        {
            // Событие без подписчиков — норма (fan-out в пустоту). Команда без владельца — нет.
            BusTelemetry.RecordDecision(Activity.Current, "no-handlers", envelope.MessageType);
            logger.LogDebug("Нет хендлеров для {MessageType}, сообщение подтверждено", envelope.MessageType);
            return ProcessingDecision.Ack;
        }

        // 3. Дедупликация: повторную доставку одного и того же MessageId не обрабатываем дважды.
        var consumerKey = source.Name;
        if (_inbox is not null && !_inbox.TryMarkProcessed(envelope.MessageId, consumerKey))
        {
            BusTelemetry.RecordDecision(Activity.Current, "duplicate-skipped", consumerKey);
            logger.LogDebug("Дубликат {MessageId} на {Consumer} — пропущен", envelope.MessageId, consumerKey);
            return ProcessingDecision.Ack;
        }

        try
        {
            return await ExecuteAsync(transportMessage, envelope, messageType, handlers, source, activity, ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _inbox?.Forget(envelope.MessageId, consumerKey);
            BusTelemetry.RecordDecision(Activity.Current, "canceled", "host shutdown");
            return ProcessingDecision.Discard("canceled: host shutdown");
        }
        catch (Exception exception)
        {
            _inbox?.Forget(envelope.MessageId, consumerKey);

            BusTelemetry.FailureRecorded(envelope, envelope.DeliveryAttempt, exception);

            // В бюджет считаются только ретраи, вызванные сбоями: осознанные откладывания
            // (debounce, defer) не являются признаком нездорового трафика (идея 162).
            var decision = Decide(envelope, exception, source);
            _retryBudget.Record(decision.Action is ProcessingAction.Retry);

            // Вторая линия обороны: ретраи исчерпаны, сообщение готовится в DLQ —
            // сначала дадим IFailedConsumer шанс (идея 169).
            if (decision.Action is ProcessingAction.DeadLetter or ProcessingAction.Discard
                && failedConsumers.For(messageType) is { } secondLine
                && await TrySecondLineAsync(envelope, messageType, exception, source, secondLine, ct).ConfigureAwait(false))
            {
                return ProcessingDecision.Ack;
            }

            return decision;
        }
    }

    /// <summary>Отдаёт сообщение, исчерпавшее все попытки, обработчику второй линии обороны.
    /// Если вторая линия упала сама — решение о DLQ сохраняется (идея 169).</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "PiiMasker — диагностический reflection-режим (legacy): под AOT выключен (PiiMaskingEnabled=false), " +
        "и эта ветка недостижима.")]
    private async ValueTask<bool> TrySecondLineAsync(
        Envelope envelope,
        Type messageType,
        Exception exception,
        TransportDestination source,
        IFailedConsumerDispatcher secondLine,
        CancellationToken ct)
    {
        var message = Deserialize(envelope, messageType);
        if (message is null)
            return false;

        var description = $"{exception.GetType().Name}: {exception.Message}";

        // Персональные данные в описании ошибки могут уйти в DLQ/уведомления второй линии:
        // если включено маскирование, сериализуем контракт с заменой PII-полей (идея 456).
        if (options.PiiMaskingEnabled)
            description = $"{description}\nContract({message.GetType().Name}): {AvtoBus.Diagnostics.PiiMasker.ToMaskedText(message)}";

        var failed = FailedMessageFactory.Create(messageType, message, envelope, description, exception, envelope.DeliveryAttempt);

        try
        {
            await using var scope = rootServices.CreateAsyncScope();
            var context = ContextFactory.Create(
                messageType,
                envelope,
                message,
                scope.ServiceProvider,
                bus,
                ct,
                source);

            await secondLine.DispatchAsync(failed, context).ConfigureAwait(false);

            BusTelemetry.RecordDecision(Activity.Current, "second-line", secondLine.HandlerName);
            logger.LogWarning(
                "Сообщение {MessageId} ({MessageType}) передано второй линии обороны: {Handler}",
                envelope.MessageId,
                envelope.MessageType,
                secondLine.HandlerName);

            return true;
        }
        catch (Exception fallbackException)
        {
            logger.LogError(
                fallbackException,
                "Вторая линия обороны упала для {MessageId} — сообщение уходит в DLQ",
                envelope.MessageId);

            return false;
        }
    }

    private async ValueTask<ProcessingDecision> ExecuteAsync(
        ITransportMessage transportMessage,
        Envelope envelope,
        Type messageType,
        IMessageDispatcher[] handlers,
        TransportDestination source,
        Activity? activity,
        CancellationToken ct)
    {
        var message = Deserialize(envelope, messageType)
                      ?? throw new InvalidOperationException(
                          $"Тело сообщения {envelope.MessageId} десериализовалось в null.");

        // Каждое сообщение получает свой DI-скоуп — как HTTP-запрос в ASP.NET Core.
        await using var scope = rootServices.CreateAsyncScope();

        var context = CreateContext(envelope, message, messageType, scope.ServiceProvider, source, ct);

        // Тенант конверта становится текущим контекстом на время обработки (идея 461):
        // каскады, публикуемые хендлером, наследуют его автоматически.
        using var tenantScope = TenantContext.Push(envelope.TenantId);

        var stopwatch = Stopwatch.StartNew();

        using var scopeLogger = logger.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = envelope.MessageId,
            ["CorrelationId"] = envelope.CorrelationId,
            ["MessageType"] = envelope.MessageType,
            ["Attempt"] = envelope.DeliveryAttempt,
        });

        // Пайплайн заканчивается вызовом хендлеров: middleware оборачивают обработку целиком.
        await pipeline(context).ConfigureAwait(false);

        stopwatch.Stop();
        RecordMetrics(envelope, source, context.Outcome, stopwatch.Elapsed);

        return await FinalizeAsync(context, envelope, source, activity, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Разбирает исход обработки и отправляет каскады. Каскады уходят строго ПОСЛЕ успеха хендлера:
    /// это и есть outbox-семантика — упало, значит ничего не улетело.
    /// </summary>
    private async ValueTask<ProcessingDecision> FinalizeAsync(
        ConsumeContext context,
        Envelope envelope,
        TransportDestination source,
        Activity? activity,
        CancellationToken ct)
    {
        switch (context.Outcome)
        {
            case ConsumeOutcome.DeadLettered:
                BusTelemetry.RecordDecision(activity, "dead-letter", context.DeadLetterReason ?? "unspecified");
                return ProcessingDecision.DeadLetter(context.DeadLetterReason ?? "хендлер запросил DLQ");

            case ConsumeOutcome.Deferred:
                var delay = context.DeferralDelay ?? TimeSpan.FromSeconds(5);
                BusTelemetry.RecordDecision(activity, "defer", $"{delay.TotalSeconds:0.##}s");
                return ProcessingDecision.Retry(delay);

            case ConsumeOutcome.Skipped or ConsumeOutcome.Superseded:
                // Осознанный пропуск: каскады не отправляем, но и ошибкой не считаем.
                BusTelemetry.RecordDecision(activity, "skipped", context.Outcome.ToString().ToLowerInvariant());
                return ProcessingDecision.Ack;
        }

        foreach (var outgoing in context.Outgoing)
        {
            await bus.DispatchAsync(
                outgoing.Message,
                outgoing.Message.GetType(),
                outgoing.Kind,
                outgoing.Options,
                envelope,
                ct).ConfigureAwait(false);
        }

        BusTelemetry.RecordDecision(activity, "ack", $"каскадов: {context.Outgoing.Count}");
        return ProcessingDecision.Ack;
    }

    /// <summary>Решает судьбу сообщения после исключения: ретрай, DLQ или отбрасывание.</summary>
    private ProcessingDecision Decide(Envelope envelope, Exception exception, TransportDestination source)
    {
        var settings = options.Recoverability;
        var retryClass = settings.Classify(exception);
        var attempt = envelope.DeliveryAttempt;

        BusTelemetry.ConsumeCount.Add(1,
            new KeyValuePair<string, object?>("messaging.message.type", envelope.MessageType),
            new KeyValuePair<string, object?>("messaging.avtobus.outcome", "failed"));

        if (retryClass is RetryClass.Permanent)
        {
            logger.LogWarning(
                exception,
                "Перманентная ошибка {MessageType} ({MessageId}) — без ретраев",
                envelope.MessageType,
                envelope.MessageId);

            return settings.OnFailureAction is FailureAction.Discard
                ? ProcessingDecision.Discard(exception.Message)
                : ProcessingDecision.DeadLetter(exception.Message, exception);
        }

        // Ретрай-бюджет: доля ретраев в трафике выше допустимой — ретраи отключаются
        // до конца окна, сообщение идёт в финальное решение (идея 162).
        if (!_retryBudget.CanRetry())
        {
            BusTelemetry.RecordDecision(Activity.Current, "retry-budget-exhausted", $"бюджет {settings.RetryBudget:0%}");
            logger.LogWarning(
                "Ретрай-бюджет исчерпан для {MessageType} ({MessageId}) — сразу финальное решение",
                envelope.MessageType,
                envelope.MessageId);

            return settings.OnFailureAction is FailureAction.Discard
                ? ProcessingDecision.Discard("retry budget exhausted")
                : ProcessingDecision.DeadLetter("retry budget exhausted", exception);
        }

        // Немедленные ретраи: транспорт возвращает сообщение сразу, без задержки.
        if (attempt <= settings.ImmediateRetryCount)
        {
            BusTelemetry.RecordDecision(Activity.Current, "retry-immediate", $"попытка {attempt}");
            BusTelemetry.RetryCount.Add(1,
                new KeyValuePair<string, object?>("messaging.message.type", envelope.MessageType),
                new KeyValuePair<string, object?>("messaging.avtobus.retry.kind", "immediate"));

            logger.LogDebug(
                exception,
                "Немедленный ретрай {MessageType} ({MessageId}), попытка {Attempt}",
                envelope.MessageType,
                envelope.MessageId,
                attempt);

            return ProcessingDecision.Retry(TimeSpan.Zero);
        }

        // Отложенные ретраи: пауза растёт по бэкоффу.
        var delayedAttempt = attempt - settings.ImmediateRetryCount;
        if (delayedAttempt <= settings.DelayedRetryCount)
        {
            var delay = settings.DelayedBackoff.Delay(delayedAttempt, _random);

            BusTelemetry.RecordDecision(Activity.Current, "retry-delayed", $"{delay.TotalSeconds:0.##}s");
            BusTelemetry.RetryCount.Add(1,
                new KeyValuePair<string, object?>("messaging.message.type", envelope.MessageType),
                new KeyValuePair<string, object?>("messaging.avtobus.retry.kind", "delayed"));

            logger.LogInformation(
                exception,
                "Отложенный ретрай {MessageType} ({MessageId}) через {Delay}, попытка {Attempt}",
                envelope.MessageType,
                envelope.MessageId,
                delay,
                attempt);

            return ProcessingDecision.Retry(delay);
        }

        logger.LogError(
            exception,
            "Исчерпаны все попытки для {MessageType} ({MessageId}) из {Source}",
            envelope.MessageType,
            envelope.MessageId,
            source.Name);

        var finalDecision = settings.OnFailureAction is FailureAction.Discard
            ? ProcessingDecision.Discard(exception.Message)
            : ProcessingDecision.DeadLetter(exception.Message, exception);

        BusTelemetry.RecordDecision(Activity.Current, "final", $"{finalDecision.Action.ToString().ToLowerInvariant()}: {exception.GetType().Name}");
        return finalDecision;
    }

    private ConsumeContext CreateContext(
        Envelope envelope,
        object message,
        Type messageType,
        IServiceProvider services,
        TransportDestination source,
        CancellationToken ct)
    {
        // ConsumeContext<T> строится через рефлексию один раз на сообщение; конструктор internal,
        // поэтому идём через кэшированную фабрику по типу.
        return ContextFactory.Create(messageType, envelope, message, services, bus, ct, source);
    }

    private object? Deserialize(Envelope envelope, Type messageType)
    {
        var serializer = options.Serializers.For(envelope.ContentType);
        return serializer.Deserialize(envelope.Body, messageType);
    }

    private static void RecordMetrics(
        Envelope envelope,
        TransportDestination source,
        ConsumeOutcome outcome,
        TimeSpan elapsed)
    {
        var tags = new TagList
        {
            { "messaging.message.type", envelope.MessageType },
            { "messaging.destination.name", source.Name },
            { "messaging.avtobus.outcome", outcome.ToString().ToLowerInvariant() },
        };

        BusTelemetry.ConsumeDuration.Record(elapsed.TotalMilliseconds, tags);
        BusTelemetry.ConsumeCount.Add(1, tags);
        BusTelemetry.ConsumeBytes.Record(envelope.Body.Length, tags);
        BusTelemetry.ConsumeRecorded(envelope.MessageType, source, (long)elapsed.TotalMilliseconds);

        // Critical time: полное время жизни сообщения, включая ожидание в очереди (идея 303).
        BusTelemetry.CriticalTime.Record((DateTimeOffset.UtcNow - envelope.SentAt).TotalMilliseconds, tags);
    }
}

/// <summary>Что транспорт должен сделать с сообщением после обработки.</summary>
public readonly record struct ProcessingDecision
{
    private ProcessingDecision(ProcessingAction action, TimeSpan delay, string? reason, Exception? exception)
    {
        Action = action;
        Delay = delay;
        Reason = reason;
        Exception = exception;
    }

    public ProcessingAction Action { get; }

    public TimeSpan Delay { get; }

    public string? Reason { get; }

    public Exception? Exception { get; }

    public static ProcessingDecision Ack { get; } = new(ProcessingAction.Acknowledge, default, null, null);

    public static ProcessingDecision Retry(TimeSpan delay) => new(ProcessingAction.Retry, delay, null, null);

    public static ProcessingDecision DeadLetter(string reason, Exception? exception = null)
        => new(ProcessingAction.DeadLetter, default, reason, exception);

    public static ProcessingDecision Poison(string reason) => new(ProcessingAction.Poison, default, reason, null);

    public static ProcessingDecision Discard(string reason) => new(ProcessingAction.Discard, default, reason, null);
}

public enum ProcessingAction
{
    Acknowledge,
    Retry,

    /// <summary>В error-очередь: упало бизнес-исключением после ретраев, можно реплеить (идея 164).</summary>
    DeadLetter,

    /// <summary>В poison-очередь: не десериализовалось или нет типа — реплей бессмыслен (идея 164).</summary>
    Poison,

    /// <summary>Выбросить без сохранения.</summary>
    Discard,
}
