using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace AvtoBus.InMemory;

/// <summary>
/// In-memory транспорт с полной семантикой брокера: очереди, топики с fan-out,
/// consumer groups, отложенная доставка, DLQ (идея 52).
///
/// Это не игрушка для тестов: на нём работает модульный монолит, а при выносе
/// модуля в отдельный сервис меняется только конфигурация (идея 27).
/// </summary>
public sealed class InMemoryTransport : ITransport,
    Runtime.IScheduleCancellable,
    AvtoBus.Observability.IQueueDepthProvider
{
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);

    /// <summary>Топик → группы консьюмеров. Каждая группа получает свою копию сообщения.</summary>
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, InMemoryQueue>> _topics =
        new(StringComparer.Ordinal);

    private readonly TimeProvider _time;
    private readonly int _capacity;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _promoter;

    public InMemoryTransport(TimeProvider? timeProvider = null, int capacity = 10_000)
    {
        _time = timeProvider ?? TimeProvider.System;
        _capacity = capacity;

        // Фоновая задача переносит созревшие отложенные сообщения в основные каналы.
        _promoter = Task.Run(PromoteDelayedLoopAsync);
    }

    public string Name => TransportNames.InMemory;

    /// <summary>Глубина всех очередей — для метрик и тестов.</summary>
    public IReadOnlyDictionary<string, int> QueueDepths
        => _queues.ToDictionary(pair => pair.Key, pair => pair.Value.Depth, StringComparer.Ordinal);

    public ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
        => destination.Kind is DestinationKind.Queue
            ? SendToQueueAsync(envelope, destination.Name, ct)
            : SendToTopicAsync(envelope, destination.Name, ct);

    private ValueTask SendToQueueAsync(Envelope envelope, string queueName, CancellationToken ct)
        => GetOrCreateQueue(queueName).EnqueueAsync(envelope, ct);

    private async ValueTask SendToTopicAsync(Envelope envelope, string topicName, CancellationToken ct)
    {
        // Fan-out: копия в очередь каждой подписанной группы. Нет подписчиков — сообщение
        // просто исчезает, ровно как в exchange без привязок.
        if (!_topics.TryGetValue(topicName, out var groups))
            return;

        foreach (var queue in groups.Values)
            await queue.EnqueueAsync(envelope, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var queue = Subscribe(subscription);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _shutdown.Token);

        await foreach (var pending in queue.ReadAllAsync(linked.Token).ConfigureAwait(false))
        {
            var envelope = pending.Envelope;

            // Протухшие сообщения не доходят до хендлера (идея 47).
            if (envelope.IsExpired(_time.GetUtcNow()))
            {
                await SendToQueueAsync(
                    envelope.WithHeader(BusHeaders.DeadLetterReason, "expired"),
                    ExpiredQueueName(queue.Name),
                    linked.Token).ConfigureAwait(false);
                continue;
            }

            yield return new InMemoryMessage(this, queue, envelope);
        }
    }

    /// <summary>
    /// Привязывает подписку к очереди. Для топика создаётся очередь на группу консьюмеров:
    /// реплики одной группы делят нагрузку, разные группы получают копии.
    /// </summary>
    private InMemoryQueue Subscribe(TransportSubscription subscription)
    {
        var destination = subscription.Destination;

        if (destination.Kind is DestinationKind.Queue)
            return GetOrCreateQueue(destination.Name);

        var groups = _topics.GetOrAdd(destination.Name, static _ => new ConcurrentDictionary<string, InMemoryQueue>(StringComparer.Ordinal));
        return groups.GetOrAdd(
            subscription.ConsumerGroup,
            group => GetOrCreateQueue($"{destination.Name}::{group}"));
    }

    public ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        // Очереди создаём заранее, чтобы отправка до подъёма консьюмера не терялась.
        foreach (var destination in destinations)
        {
            if (destination.Kind is DestinationKind.Queue)
            {
                GetOrCreateQueue(destination.Name);
                GetOrCreateQueue(ErrorQueueName(destination.Name));
                GetOrCreateQueue(PoisonQueueName(destination.Name));
            }
            else
            {
                _topics.GetOrAdd(destination.Name, static _ => new ConcurrentDictionary<string, InMemoryQueue>(StringComparer.Ordinal));
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Регистрирует группу-подписчика топика до старта консьюмера, чтобы не терять ранние события.</summary>
    public void BindSubscription(string topicName, string consumerGroup)
        => Subscribe(new TransportSubscription(TransportDestination.Topic(topicName), consumerGroup));

    internal InMemoryQueue GetOrCreateQueue(string name)
        => _queues.GetOrAdd(name, key => new InMemoryQueue(key, _capacity, _time));

    /// <summary>Отменяет отложенное сообщение во всех очередях (идея 46).</summary>
    public bool CancelScheduled(Guid messageId)
    {
        var cancelled = false;
        foreach (var queue in _queues.Values)
            cancelled |= queue.CancelDelayed(messageId);

        return cancelled;
    }

    /// <summary>Переносит созревшие отложенные сообщения. Шаг мелкий — задержки точны до ~20 мс.</summary>
    private async Task PromoteDelayedLoopAsync()
    {
        try
        {
            while (!_shutdown.IsCancellationRequested)
            {
                foreach (var queue in _queues.Values)
                    await queue.PromoteDueAsync(_shutdown.Token).ConfigureAwait(false);

                await Task.Delay(TimeSpan.FromMilliseconds(20), _time, _shutdown.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Штатная остановка.
        }
        catch (ObjectDisposedException)
        {
            // _shutdown disposed before loop exit.
        }
    }

    /// <summary>Принудительно проталкивает отложенные сообщения — нужно тестам с виртуальным временем.</summary>
    public async ValueTask PumpDelayedAsync(CancellationToken ct = default)
    {
        foreach (var queue in _queues.Values)
            await queue.PromoteDueAsync(ct).ConfigureAwait(false);
    }

    public static string ErrorQueueName(string queue) => $"{queue}.error";

    public static string PoisonQueueName(string queue) => $"{queue}.poison";

    public static string ExpiredQueueName(string queue) => $"{queue}.expired";

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        // Транспорт зарегистрирован в контейнере и как ITransport, и как конкретный тип,
        // поэтому Dispose может прийти дважды на один и тот же экземпляр.
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await _shutdown.CancelAsync().ConfigureAwait(false);

        try
        {
            await _promoter.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо при остановке.
        }

        foreach (var queue in _queues.Values)
            queue.Complete();

        foreach (var queue in _queues.Values)
            queue.Dispose();

        _shutdown.Dispose();
    }

    /// <summary>
    /// Сообщение из in-memory очереди. Reject с requeue возвращает его в ту же очередь,
    /// без requeue — отправляет в error-очередь.
    /// </summary>
    private sealed class InMemoryMessage(InMemoryTransport transport, InMemoryQueue queue, Envelope envelope)
        : ITransportMessage
    {
        private int _settled;

        public Envelope Envelope { get; } = envelope;

        /// <summary>Физическая очередь, из которой вычитано сообщение (для топика — очередь группы).</summary>
        public TransportDestination Source { get; } = TransportDestination.Queue(queue.Name);

        public ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            // Ack in-memory — это просто «не возвращать»: сообщение уже вычитано из канала.
            Interlocked.Exchange(ref _settled, 1);
            return ValueTask.CompletedTask;
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue)
            {
                await queue.EnqueueAsync(Envelope.NextAttempt(), ct).ConfigureAwait(false);
                return;
            }

            var errorQueue = transport.GetOrCreateQueue(ErrorQueueName(queue.Name));
            await errorQueue.EnqueueAsync(Envelope, ct).ConfigureAwait(false);
        }
    }
}
