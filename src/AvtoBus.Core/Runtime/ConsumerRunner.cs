using System.Threading.Channels;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace AvtoBus.Runtime;

/// <summary>
/// Качает сообщения одной подписки. Параллелизм ограничен настройкой консьюмера,
/// упорядочивание по ключу — партиционированием.
/// </summary>
public sealed class ConsumerRunner(
    ConsumerSubscription subscription,
    MessageProcessor processor,
    BusOptions options,
    TimeProvider time,
    ILogger logger) : IAsyncDisposable
{
    private readonly CircuitBreaker _breaker = new(
        options.CircuitBreakerThreshold,
        options.CircuitBreakerDuration,
        time);

    private readonly SemaphoreSlim _inFlight = new(
        subscription.Settings?.MaxParallelism ?? 1,
        subscription.Settings?.MaxParallelism ?? 1);

    private readonly CancellationTokenSource _receiveCts = new();

    private PartitionRouter? _router;

    /// <summary>Задача цикла приёма: завершается, когда ранер перестал вычитывать новые сообщения.</summary>
    public Task? RunTask { get; internal set; }

    /// <summary>Останавливает приём новых сообщений; начатые обработки продолжают работать.</summary>
    public void StopReceiving()
    {
        // Идемпотентно и безопасно после Dispose: повторный StopAsync хоста
        // (тесты + Host.StopAsync при Dispose харнесса) не должен падать.
        try { _receiveCts.Cancel(); }
        catch (ObjectDisposedException) { }
    }

    private long _processed;
    private long _failed;

    public string Name => subscription.Subscription.Destination.Name;

    /// <summary>Отставание: необработанные сообщения в источнике подписки (идея 302).</summary>
    public long Lag
    {
        get
        {
            var destination = subscription.Subscription.Destination;

            if (destination.Kind == DestinationKind.Topic
                && subscription.Transport is ITopicDepthProvider topics
                && topics.TopicDepths.TryGetValue(destination.Name, out var topicDepth))
            {
                return topicDepth;
            }

            if (subscription.Transport is not IQueueDepthProvider depth)
                return 0;

            // Для топика без TopicDepthProvider читаем физическую очередь группы.
            var key = destination.Kind == DestinationKind.Topic
                ? $"{destination.Name}::{subscription.Subscription.ConsumerGroup}"
                : destination.Name;

            return depth.QueueDepths.TryGetValue(key, out var queueDepth) ? queueDepth : 0;
        }
    }

    public CircuitState CircuitState => _breaker.State;

    public long Processed => Interlocked.Read(ref _processed);

    public long Failed => Interlocked.Read(ref _failed);

    /// <summary>Пауза консьюмера без рестарта процесса (идея 36).</summary>
    private volatile bool _isPaused;
    public bool IsPaused { get => _isPaused; set => _isPaused = value; }

    public async Task RunAsync(CancellationToken ct)
    {
        var partitions = subscription.Settings?.Partitions ?? 0;

        // Receive-токен связывает штатную остановку (ct) с дрейн-остановкой (StopReceiving):
        // при дрейн-остановке новые сообщения не берутся, а in-flight продолжают работать.
        using var receive = CancellationTokenSource.CreateLinkedTokenSource(_receiveCts.Token, ct);

        // Партиционированная обработка: сообщения одного ключа идут строго последовательно (идея 25).
        var router = partitions > 0
            ? new PartitionRouter(partitions, subscription.Settings!.PartitionKeySelector, logger: logger)
            : null;
        _router = router;

        try
        {
            await foreach (var message in subscription.Transport
                               .ReceiveAsync(subscription.Subscription, receive.Token)
                               .ConfigureAwait(false))
            {
                await WaitWhilePausedAsync(receive.Token).ConfigureAwait(false);
                await WaitForCircuitAsync(receive.Token).ConfigureAwait(false);

                if (router is not null)
                {
                    await router.EnqueueAsync(message, HandleAsync, receive.Token).ConfigureAwait(false);
                    continue;
                }

                await _inFlight.WaitAsync(receive.Token).ConfigureAwait(false);

                _ = Task.Run(async () =>
                {
                    try { await HandleAsync(message, ct).ConfigureAwait(false); }
                    finally { _inFlight.Release(); }
                }, CancellationToken.None);
            }
        }
        catch (OperationCanceledException) when (receive.IsCancellationRequested)
        {
            // Штатная остановка (ct) или дрейн (StopReceiving).
        }
        finally
        {
            router?.Complete();
        }
    }

    private async Task HandleAsync(ITransportMessage message, CancellationToken ct)
    {
        // Источник — фактическая очередь, из которой вычитано сообщение: для топика это
        // очередь группы консьюмеров, и DLQ/retry наследуют её имя (идея 164).
        var source = message.Source;

        try
        {
            var decision = await processor.ProcessAsync(message, source, ct).ConfigureAwait(false);
            await ApplyAsync(message, decision, ct).ConfigureAwait(false);

            if (decision.Action is ProcessingAction.Acknowledge)
            {
                Interlocked.Increment(ref _processed);
                _breaker.RecordSuccess();
            }
            else
            {
                Interlocked.Increment(ref _failed);
                _breaker.RecordFailure();
            }
        }
        catch (Exception exception)
        {
            // Гарантируем settlement даже если процессор выбросил (дефект): иначе сообщение зависнет unacked.
            logger.LogError(exception, "Сбой транспорта при обработке сообщения из {Source}", source.Name);
            _breaker.RecordFailure();
            try
            {
                var fallback = ProcessingDecision.Poison($"transport failure: {exception.Message}");
                await ApplyAsync(message, fallback, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex2)
            {
                logger.LogError(ex2, "Не удалось отправить poison для {Source}", source.Name);
                try { await message.RejectAsync(requeue: false, CancellationToken.None).ConfigureAwait(false); } catch { }
            }
        }
    }

    /// <summary>Применяет решение процессора к сообщению на уровне транспорта.</summary>
    private async ValueTask ApplyAsync(ITransportMessage message, ProcessingDecision decision, CancellationToken ct)
    {
        var source = message.Source;
        // Ack/Reject/Send не должны зависеть от stoppingToken — при StopAsync он отменён,
        // иначе сообщение останется unacked и уйдёт в дубликат (идея 35).
        var settleCt = CancellationToken.None;

        switch (decision.Action)
        {
            case ProcessingAction.Acknowledge:
                await message.AcknowledgeAsync(settleCt).ConfigureAwait(false);
                break;

            case ProcessingAction.Retry:
                if (decision.Delay > TimeSpan.Zero)
                {
                    // Задержку перед повтором делаем через DeliverAt: сообщение вернётся в очередь,
                    // но станет видимым только по истечении бэкоффа.
                    await RequeueWithDelayAsync(message, decision.Delay, source, settleCt).ConfigureAwait(false);
                    break;
                }

                await message.RejectAsync(requeue: true, settleCt).ConfigureAwait(false);
                break;

            case ProcessingAction.DeadLetter:
                await DeadLetterAsync(message, decision, source, InMemoryErrorSuffix, settleCt).ConfigureAwait(false);
                break;

            case ProcessingAction.Poison:
                await DeadLetterAsync(message, decision, source, PoisonSuffix, settleCt).ConfigureAwait(false);
                break;

            case ProcessingAction.Discard:
                logger.LogWarning(
                    "Сообщение {MessageId} отброшено: {Reason}",
                    message.Envelope.MessageId,
                    decision.Reason);
                await message.AcknowledgeAsync(settleCt).ConfigureAwait(false);
                break;
        }
    }

    private const string InMemoryErrorSuffix = "error";
    private const string PoisonSuffix = "poison";

    private async ValueTask RequeueWithDelayAsync(
        ITransportMessage message,
        TimeSpan delay,
        TransportDestination source,
        CancellationToken ct)
    {
        var delayed = message.Envelope.NextAttempt() with { DeliverAt = time.GetUtcNow() + delay };

        await subscription.Transport
            .SendAsync(delayed, source, ct)
            .ConfigureAwait(false);

        await message.AcknowledgeAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Отправляет сообщение в error/poison-очередь с полным контекстом отказа (идея 165):
    /// стектрейс, исходная очередь, время — всё, что нужно для разбора и реплея.
    /// </summary>
    private async ValueTask DeadLetterAsync(
        ITransportMessage message,
        ProcessingDecision decision,
        TransportDestination source,
        string suffix,
        CancellationToken ct)
    {
        var headers = new List<KeyValuePair<string, string>>
        {
            new(BusHeaders.DeadLetterReason, decision.Reason ?? "unspecified"),
            new(BusHeaders.FailedQueue, source.Name),
            new(BusHeaders.FailedAt, time.GetUtcNow().ToString("O")),
            new(BusHeaders.OriginalDestination, source.ToString()),
        };
        if (decision.Exception is { } exception)
        {
            headers.Add(new(BusHeaders.ExceptionType, exception.GetType().FullName ?? exception.GetType().Name));
            headers.Add(new(BusHeaders.ExceptionMessage, exception.Message));
            headers.Add(new(BusHeaders.ExceptionStackTrace, exception.StackTrace ?? string.Empty));
        }
        var enriched = message.Envelope.WithHeaders(headers);

        await subscription.Transport
            .SendAsync(enriched, TransportDestination.Queue($"{source.Name}.{suffix}"), ct)
            .ConfigureAwait(false);

        await message.AcknowledgeAsync(ct).ConfigureAwait(false);

        Observability.BusTelemetry.DeadLetterCount.Add(1,
            new KeyValuePair<string, object?>("messaging.message.type", message.Envelope.MessageType),
            new KeyValuePair<string, object?>("messaging.destination.name", source.Name));
    }

    private async Task WaitWhilePausedAsync(CancellationToken ct)
    {
        while (IsPaused && !ct.IsCancellationRequested)
            await Task.Delay(TimeSpan.FromMilliseconds(50), time, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Пока цепь разомкнута, консьюмер спит — сообщения остаются в брокере, а не сгорают (идея 163).
    /// </summary>
    private async Task WaitForCircuitAsync(CancellationToken ct)
    {
        while (!_breaker.CanProcess() && !ct.IsCancellationRequested)
        {
            var wait = _breaker.RetryAfter();
            if (wait <= TimeSpan.Zero)
                break;

            logger.LogWarning("Цепь разомкнута для {Consumer}, пауза {Wait}", Name, wait);
            await Task.Delay(wait, time, ct).ConfigureAwait(false);
        }
    }

    /// <summary>Дожидается завершения обработок, начатых до остановки.</summary>
    public async Task DrainAsync(CancellationToken ct)
    {
        if (_router is not null)
        {
            await _router.DrainAsync(ct).ConfigureAwait(false);
            return;
        }

        var maxParallelism = subscription.Settings?.MaxParallelism ?? 1;
        var acquired = 0;
        try
        {
            for (var i = 0; i < maxParallelism; i++)
            {
                await _inFlight.WaitAsync(ct).ConfigureAwait(false);
                acquired++;
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Дрейн {Consumer} прерван по таймауту", Name);
        }
        finally
        {
            if (acquired > 0) _inFlight.Release(acquired);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _receiveCts.Dispose();
        _inFlight.Dispose();
        if (_router is not null) await _router.DisposeAsync().ConfigureAwait(false);
    }
}
