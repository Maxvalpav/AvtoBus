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
    private int _stopped;

    /// <summary>Атомарный снапшот ранеров: публикуется один раз после старта (аудит C1).</summary>
    private volatile ConsumerRunner[] _snapshot = [];

    /// <summary>Состояние консьюмеров — для health-модели и дашборда (идея 49).</summary>
    public IReadOnlyList<ConsumerRunner> Runners => _snapshot;

    /// <summary>Цикл приёма каждого ранера завершён (остановка получена или дрейн).</summary>
    public bool AllReceivingStopped => _snapshot.Length > 0 && _snapshot.All(runner => runner.RunTask is { IsCompleted: true });

    /// <summary>
    /// Lag каждой подписки: сколько сообщений ещё ждёт в источнике. Для топика считаем
    /// глубину физической очереди группы <c>{topic}::{group}</c> — точную картину даёт
    /// только транспорт, читающий её напрямую (идея 302).
    /// </summary>
    public IReadOnlyDictionary<string, long> ConsumerLags
    {
        get
        {
            var snapshot = _snapshot;
            var lags = new Dictionary<string, long>(snapshot.Length, StringComparer.Ordinal);
            foreach (var runner in snapshot)
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
            // Fail-fast: MaxParallelism<=0 (напрямую в ConsumerSettings, мимо fluent-гарда)
            // иначе давал тихий stall — WaitAsync/DrainAsync висели до таймаута.
            var parallelism = subscription.Settings?.MaxParallelism ?? 1;
            if (parallelism < 1)
                throw new InvalidOperationException(
                    $"MaxParallelism должен быть >= 1 (подписка '{subscription.Subscription.Destination.Name}').");
            var runner = new ConsumerRunner(subscription, processor, options, time, logger);
            _runners.Add(runner);
        }

        // Reply-очередь этого процесса слушаем на КАЖДОМ транспорте (аудит C2):
        // ответ может прийти любым транспортом (у отвечающей стороны своя
        // маршрутизация типа ответа), а не только транспортом по умолчанию.
        foreach (var transport in transports.All)
        {
            var replySubscription = new ConsumerSubscription(
                transport,
                new TransportSubscription(TransportDestination.Queue(replies.ReplyAddress), options.ServiceName),
                MessageType: null);

            _runners.Add(new ConsumerRunner(replySubscription, processor, options, time, logger));
        }

        // Публикуем снапшот атомарно: читатели Runners/ConsumerLags/StopAsync видят
        // либо пустой, либо полный набор — без InvalidOperationException при перечислении List (аудит C1).
        _snapshot = _runners.ToArray();

        logger.LogInformation("AvtoBus запущен: {Count} консьюмеров", _snapshot.Length);

        // Стартовые предупреждения конфигурации (experimental-пакеты, in-memory в
        // Production и т.п.): видны в логах каждого подъёма, пропустить сложно.
        foreach (var warning in options.StartupWarnings)
            logger.LogWarning("AvtoBus: {Warning}", warning);

        // Запускаем все циклы приёма; их задачи нужны и для сигнала «приём остановлен» (идея 35).
        foreach (var runner in _snapshot)
            runner.RunTask = runner.RunAsync(stoppingToken);

        try
        {
            await Task.WhenAll(_snapshot.Select(runner => runner.RunTask!)).ConfigureAwait(false);
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

        // Reply-очередь создаём на каждом транспорте — ответы слушаем везде (аудит C2).
        foreach (var transport in transports.All)
            await transport
                .ProvisionAsync([TransportDestination.Queue(replies.ReplyAddress)], ct)
                .ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // StopAsync могут вызвать дважды (вручную + Host.StopAsync при dispose):
        // дрейн и Dispose ранеров выполняем один раз.
        if (Interlocked.Exchange(ref _stopped, 1) == 1)
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        // 1. Перестаём брать новые сообщения. In-flight обработки продолжают работать:
        // их доделываем, а не отменяем (идея 35). Читаем снапшот — старт мог ещё не опубликовать его.
        foreach (var runner in _snapshot)
            runner.StopReceiving();

        // 2. Дрейн: ждём завершения начатых обработок в пределах ShutdownDrainTimeout.
        // Отрицательный таймаут ронял бы StopAsync в ArgumentOutOfRangeException —
        // клампим с предупреждением вместо сломанной остановки.
        var drainTimeout = options.ShutdownDrainTimeout;
        if (drainTimeout < TimeSpan.Zero)
        {
            logger.LogWarning("ShutdownDrainTimeout отрицательный ({Timeout}) — использован 30с по умолчанию", drainTimeout);
            drainTimeout = TimeSpan.FromSeconds(30);
        }
        using var drain = new CancellationTokenSource(drainTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drain.Token);

        foreach (var runner in _snapshot)
            await runner.DrainAsync(linked.Token).ConfigureAwait(false);

        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        // Ранеры держат CTS + SemaphoreSlim + PartitionRouter: без Dispose они текли
        // до конца жизни хоста (заметно в тестах с пересозданием хостов).
        // Дрейн уже завершён, поэтому DisposeAsync возвращается сразу.
        foreach (var runner in _snapshot)
        {
            try { await runner.DisposeAsync().ConfigureAwait(false); }
            catch (Exception ex) { logger.LogDebug(ex, "Ошибка Dispose ранера {Consumer}", runner.Name); }
        }
    }
}
