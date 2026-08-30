using System.Threading.Channels;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Runtime;

/// <summary>
/// Поднимает консьюмеров, качает из них сообщения и корректно останавливается с дрейном (идея 35).
/// Также источник метрики consumer lag: отставание консьюмера = глубина очереди/топика, который он читает.
/// </summary>
public sealed class ConsumerHost(
    BusOptions options,
    DispatcherRegistry dispatchers,
    TransportRegistry transports,
    MessageProcessor processor,
    ReplyRouter replies,
    TimeProvider time,
    ILogger<ConsumerHost> logger)
    : BackgroundService, AvtoBus.Observability.IConsumerLagProvider
{
    private readonly List<ConsumerRunner> _runners = [];

    /// <summary>Состояние консьюмеров — для health-модели и дашборда (идея 49).</summary>
    public IReadOnlyList<ConsumerRunner> Runners => _runners;

    /// <summary>Цикл приёма каждого ранера завершён (остановка получена или дрейн).</summary>
    public bool AllReceivingStopped => _runners.Count > 0 && _runners.All(runner => runner.RunTask is { IsCompleted: true });

    /// <summary>
    /// Lag каждой подписки: сколько сообщений ещё ждёт в источнике. Для топика считаем
    /// глубину физической очереди группы <c>{topic}::{group}</c> — точную картину даёт
    /// только транспорт, читающий её напрямую (идея 302).
    /// </summary>
    public IReadOnlyDictionary<string, long> ConsumerLags
    {
        get
        {
            var lags = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var runner in _runners)
                lags[runner.Name] = runner.Lag;
            return lags;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriptions = BuildSubscriptions();

        // Топология создаётся до подъёма консьюмеров: очередь должна существовать
        // раньше, чем в неё кто-то напишет (идея 40).
        await ProvisionAsync(subscriptions, stoppingToken).ConfigureAwait(false);

        foreach (var subscription in subscriptions)
        {
            var runner = new ConsumerRunner(subscription, processor, options, time, logger);
            _runners.Add(runner);
        }

        // Отдельный ранер слушает reply-очередь этого процесса.
        var replySubscription = new ConsumerSubscription(
            transports.Default,
            new TransportSubscription(TransportDestination.Queue(replies.ReplyAddress), options.ServiceName),
            MessageType: null);

        _runners.Add(new ConsumerRunner(replySubscription, processor, options, time, logger));

        logger.LogInformation("AvtoBus запущен: {Count} консьюмеров", _runners.Count);

        // Запускаем все циклы приёма; их задачи нужны и для сигнала «приём остановлен» (идея 35).
        foreach (var runner in _runners)
            runner.RunTask = runner.RunAsync(stoppingToken);

        try
        {
            await Task.WhenAll(_runners.Select(runner => runner.RunTask!)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("AvtoBus останавливается, дожидаемся in-flight сообщений");
        }
    }

    /// <summary>
    /// Собирает подписки: очередь на каждую команду, топик на каждое событие.
    ///
    /// Подписываемся на назначения КОНКРЕТНЫХ типов, а не на тип, указанный в хендлере:
    /// хендлер на интерфейсе <c>IOrderEvent</c> должен получать сообщения из топиков всех
    /// его наследников, ведь публикуются именно они (идея 9).
    /// </summary>
    private List<ConsumerSubscription> BuildSubscriptions()
    {
        var subscriptions = new List<ConsumerSubscription>();
        var seen = new HashSet<(string Transport, TransportDestination Destination, string Group)>();

        foreach (var messageType in ConcreteHandledTypes())
        {
            var settings = SettingsFor(messageType);

            var kind = typeof(ICommand).IsAssignableFrom(messageType)
                ? OutgoingKind.Send
                : OutgoingKind.Publish;

            var route = options.Routing.Resolve(messageType, kind);

            var destination = route.Destination.Name is not null
                ? route.Destination
                : RoutingTable.Conventional(messageType, kind);

            if (settings.QueueName is { } explicitQueue)
                destination = TransportDestination.Queue(explicitQueue);

            var transport = transports.Get(route.Transport);
            var group = settings.ConsumerGroup ?? options.ServiceName;

            // Изоляция тенантов на уровне хранилища (идея 462, уровень B/C): подписка
            // расширяется на изолированные очереди каждого зарегистрированного тенанта —
            // консьюмер читает и общую очередь, и per-tenant. Shared-тенант возвращает
            // исходное назначение, поэтому дедупликация через seen сработает сама.
            foreach (var candidate in SubscriptionDestinations(destination, group, settings.PrefetchCount))
            {
                // Два хендлера на один топик — одна подписка: диспетчеризацию по типам
                // делает MessageProcessor, дублировать вычитку не нужно.
                if (!seen.Add((transport.Name, candidate.Destination, candidate.Group)))
                    continue;

                subscriptions.Add(new ConsumerSubscription(
                    transport,
                    candidate.Subscription,
                    messageType,
                    settings));
            }
        }

        return subscriptions;
    }

    /// <summary>
    /// Все назначения, на которые читает подписка: базовая очередь плюс изолированные
    /// очереди зарегистрированных тенантов (если включена политика изоляции).
    /// </summary>
    private IEnumerable<(TransportDestination Destination, string Group, TransportSubscription Subscription)>
        SubscriptionDestinations(TransportDestination destination, string group, int prefetch)
    {
        yield return (destination, group,
            new TransportSubscription(destination, group, prefetch));

        if (options.TenantIsolationPolicy is { } isolation)
        {
            foreach (var tenantId in isolation.TenantIds)
            {
                var isolated = isolation.Isolate(destination, tenantId);
                if (isolated == destination)
                    continue;

                yield return (isolated, group,
                    new TransportSubscription(isolated, group, prefetch));
            }
        }
    }

    /// <summary>
    /// Конкретные типы, для которых есть хендлер: сам тип или его база/интерфейс.
    /// Абстракции отбрасываем — на проводе их не бывает.
    /// </summary>
    private IEnumerable<Type> ConcreteHandledTypes()
    {
        var candidates = options.ContractTypes
            .Concat(dispatchers.HandledTypes)
            .Distinct();

        foreach (var type in candidates)
        {
            if (type.IsInterface || type.IsAbstract)
                continue;

            if (dispatchers.HasHandlerFor(type))
                yield return type;
        }
    }

    /// <summary>
    /// Настройки конкретного типа, а если их нет — настройки базового типа,
    /// на котором объявлен полиморфный хендлер.
    /// </summary>
    private ConsumerSettings SettingsFor(Type messageType)
    {
        if (options.Consumers.TryGetValue(messageType, out var exact))
            return exact;

        foreach (var (candidate, settings) in options.Consumers)
        {
            if (candidate != messageType && candidate.IsAssignableFrom(messageType))
                return settings;
        }

        return new ConsumerSettings { MessageType = messageType };
    }

    private async Task ProvisionAsync(List<ConsumerSubscription> subscriptions, CancellationToken ct)
    {
        foreach (var group in subscriptions.GroupBy(s => s.Transport))
        {
            var destinations = group.Select(s => s.Subscription.Destination).Distinct().ToArray();
            await group.Key.ProvisionAsync(destinations, ct).ConfigureAwait(false);
        }

        // Reply-очередь тоже нужно создать заранее.
        await transports.Default
            .ProvisionAsync([TransportDestination.Queue(replies.ReplyAddress)], ct)
            .ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // 1. Перестаём брать новые сообщения. In-flight обработки продолжают работать:
        // их доделываем, а не отменяем (идея 35).
        foreach (var runner in _runners)
            runner.StopReceiving();

        // 2. Дрейн: ждём завершения начатых обработок в пределах ShutdownDrainTimeout.
        using var drain = new CancellationTokenSource(options.ShutdownDrainTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drain.Token);

        foreach (var runner in _runners)
            await runner.DrainAsync(linked.Token).ConfigureAwait(false);

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <param name="MessageType">Тип сообщения или <c>null</c> для служебных очередей (reply).</param>
public sealed record ConsumerSubscription(
    ITransport Transport,
    TransportSubscription Subscription,
    Type? MessageType,
    ConsumerSettings? Settings = null);

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
    public void StopReceiving() => _receiveCts.Cancel();

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
    public bool IsPaused { get; set; }

    public async Task RunAsync(CancellationToken ct)
    {
        var partitions = subscription.Settings?.Partitions ?? 0;

        // Receive-токен связывает штатную остановку (ct) с дрейн-остановкой (StopReceiving):
        // при дрейн-остановке новые сообщения не берутся, а in-flight продолжают работать.
        using var receive = CancellationTokenSource.CreateLinkedTokenSource(_receiveCts.Token, ct);

        // Партиционированная обработка: сообщения одного ключа идут строго последовательно (идея 25).
        var router = partitions > 0
            ? new PartitionRouter(partitions, subscription.Settings!.PartitionKeySelector)
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

                _ = HandleAsync(message, ct).ContinueWith(
                    _ => _inFlight.Release(),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
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
            // Сюда попадаем только при сбое самого транспорта: processor исключений не выпускает.
            logger.LogError(exception, "Сбой транспорта при обработке сообщения из {Source}", source.Name);
            _breaker.RecordFailure();
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
        var enriched = message.Envelope
            .WithHeader(BusHeaders.DeadLetterReason, decision.Reason ?? "unspecified")
            .WithHeader(BusHeaders.FailedQueue, source.Name)
            .WithHeader(BusHeaders.FailedAt, time.GetUtcNow().ToString("O"))
            .WithHeader(BusHeaders.OriginalDestination, source.ToString());

        if (decision.Exception is { } exception)
        {
            enriched = enriched
                .WithHeader(BusHeaders.ExceptionType, exception.GetType().FullName ?? exception.GetType().Name)
                .WithHeader(BusHeaders.ExceptionMessage, exception.Message)
                .WithHeader(BusHeaders.ExceptionStackTrace, exception.StackTrace ?? string.Empty);
        }

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
        // Партиционированный роутер держит «слоты» в своих воркерах: ждём их напрямую.
        if (_router is not null)
        {
            await _router.DrainAsync(ct).ConfigureAwait(false);
            return;
        }

        var slots = subscription.Settings?.MaxParallelism ?? 1;

        for (var i = 0; i < slots; i++)
        {
            try
            {
                await _inFlight.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("Дрейн {Consumer} прерван по таймауту", Name);
                return;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _receiveCts.Dispose();
        _inFlight.Dispose();
        if (_router is not null) await _router.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Раскладывает сообщения по партициям-каналам: один ключ — всегда один канал,
/// поэтому порядок в рамках ключа сохраняется при параллельной обработке разных ключей (идея 25).
/// </summary>
internal sealed class PartitionRouter : IAsyncDisposable
{
    private readonly Channel<(ITransportMessage Message, Func<ITransportMessage, CancellationToken, Task> Handler)>[] _partitions;
    private readonly Task[] _workers;
    private readonly Func<object, string>? _keySelector;

    public PartitionRouter(int partitions, Func<object, string>? keySelector, int boundedCapacity = 100)
    {
        _keySelector = keySelector;
        _partitions = new Channel<(ITransportMessage, Func<ITransportMessage, CancellationToken, Task>)>[partitions];
        _workers = new Task[partitions];

        for (var i = 0; i < partitions; i++)
        {
            var channel = Channel.CreateBounded<(ITransportMessage, Func<ITransportMessage, CancellationToken, Task>)>(
                new BoundedChannelOptions(boundedCapacity) { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });

            _partitions[i] = channel;
            _workers[i] = Task.Run(async () =>
            {
                await foreach (var (message, handler) in channel.Reader.ReadAllAsync().ConfigureAwait(false))
                    await handler(message, CancellationToken.None).ConfigureAwait(false);
            }, CancellationToken.None);
        }
    }

    public ValueTask EnqueueAsync(
        ITransportMessage message,
        Func<ITransportMessage, CancellationToken, Task> handler,
        CancellationToken ct)
    {
        var index = PartitionOf(message);
        return _partitions[index].Writer.WriteAsync((message, handler), ct);
    }

    /// <summary>
    /// Ключ берём из конверта: он проставлен на отправке и доступен без десериализации тела.
    /// Нет ключа — распределяем по MessageId, порядок в этом случае не гарантируется.
    /// </summary>
    private int PartitionOf(ITransportMessage message)
    {
        var key = message.Envelope.PartitionKey ?? message.Envelope.MessageId.ToString();
        var hash = (uint)key.GetHashCode(StringComparison.Ordinal);
        return (int)(hash % (uint)_partitions.Length);
    }

    public void Complete()
    {
        foreach (var partition in _partitions)
            partition.Writer.TryComplete();
    }

    /// <summary>Дожидается завершения всех партиций: канал закрыт, воркеры дочитали остаток.</summary>
    public async Task DrainAsync(CancellationToken ct)
    {
        foreach (var partition in _partitions)
            await partition.Reader.Completion.WaitAsync(ct).ConfigureAwait(false);

        await Task.WhenAll(_workers).WaitAsync(ct).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
