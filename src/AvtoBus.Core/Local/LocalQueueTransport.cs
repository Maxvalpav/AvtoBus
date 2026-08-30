using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace AvtoBus.Local;

/// <summary>Настройки локальной in-process очереди (идея 15).</summary>
public sealed record LocalQueueSettings(string Name, int Capacity = 10_000);

/// <summary>
/// In-process транспорт (идея 15): именованные bounded-очереди внутри одного процесса, без брокера.
/// Отличается от <c>inmemory</c> назначением, а не механикой: сообщение живёт только здесь —
/// фоновая задача внутри сервиса, перенести которую в отдельный модуль позже нельзя, и ей не нужен
/// внешний exchange. Очередь создаётся при старте, back-pressure идёт по построению канала (идея 353).
/// </summary>
public sealed class LocalQueueTransport : ITransport, AvtoBus.Observability.IQueueDepthProvider
{
    private readonly ConcurrentDictionary<string, LocalQueue> _queues = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _promoter;

    public LocalQueueTransport(TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _promoter = Task.Run(PromoteDelayedLoopAsync);
    }

    /// <summary>Предсоздаёт зарегистрированные очереди с их ёмкостями (вызывается из <c>AddLocalQueue</c>).</summary>
    public LocalQueueTransport(IEnumerable<LocalQueueSettings> queues, TimeProvider? timeProvider = null)
        : this(timeProvider)
    {
        foreach (var queue in queues)
            _queues[queue.Name] = new LocalQueue(queue.Name, queue.Capacity, _time);
    }

    public string Name => "local";

    /// <summary>Глубина всех локальных очередей — для метрики <c>avtobus.queue.depth</c> (идея 94).</summary>
    public IReadOnlyDictionary<string, int> QueueDepths
        => _queues.ToDictionary(pair => pair.Key, pair => pair.Value.Depth, StringComparer.Ordinal);

    public ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
        => GetOrCreateQueue(destination.Name).EnqueueAsync(envelope, ct);

    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var queue = GetOrCreateQueue(subscription.Destination.Name);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _shutdown.Token);

        await foreach (var envelope in queue.ReadAllAsync(linked.Token).ConfigureAwait(false))
        {
            // Протухшие сообщения не доходят до хендлера (идея 47).
            if (envelope.IsExpired(_time.GetUtcNow()))
            {
                await GetOrCreateQueue(ExpiredQueueName(queue.Name))
                    .EnqueueAsync(envelope.WithHeader(BusHeaders.DeadLetterReason, "expired"), linked.Token)
                    .ConfigureAwait(false);
                continue;
            }

            yield return new LocalQueueMessage(this, queue, envelope);
        }
    }

    public ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        // Очереди создаём заранее, чтобы отправка до подъёма консьюмера не терялась.
        foreach (var destination in destinations)
        {
            if (destination.Kind is not DestinationKind.Queue)
                continue;

            GetOrCreateQueue(destination.Name);
            GetOrCreateQueue(ErrorQueueName(destination.Name));
            GetOrCreateQueue(PoisonQueueName(destination.Name));
        }

        return ValueTask.CompletedTask;
    }

    internal LocalQueue GetOrCreateQueue(string name)
        => _queues.GetOrAdd(name, key => new LocalQueue(key, DefaultCapacity, _time));

    private const int DefaultCapacity = 10_000;

    public static string ErrorQueueName(string queue) => $"{queue}.error";

    public static string PoisonQueueName(string queue) => $"{queue}.poison";

    public static string ExpiredQueueName(string queue) => $"{queue}.expired";

    /// <summary>Переносит созревшие отложенные сообщения в основные каналы.</summary>
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
    }

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
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

        _shutdown.Dispose();
    }

    /// <summary>Сообщение из локальной очереди. Reject с requeue возвращает его в ту же очередь,
    /// без requeue — отправляет в error-очередь (обработка — в core, идея 164).</summary>
    private sealed class LocalQueueMessage(
        LocalQueueTransport transport,
        LocalQueue queue,
        Envelope envelope) : ITransportMessage
    {
        private int _settled;

        public Envelope Envelope { get; } = envelope;

        /// <summary>Физическая локальная очередь, из которой вычитано сообщение.</summary>
        public TransportDestination Source { get; } = TransportDestination.Queue(queue.Name);

        public ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            // Ack in-process — «не возвращать»: сообщение уже вычитано из канала.
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

/// <summary>
/// Одна локальная очередь. Bounded-канал даёт back-pressure по построению (идея 353):
/// переполнение блокирует паблишера, а не съедает память.
/// </summary>
internal sealed class LocalQueue(string name, int capacity, TimeProvider time)
{
    private readonly Channel<Envelope> _channel = Channel.CreateBounded<Envelope>(
        new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        });

    /// <summary>Сообщения, ожидающие наступления <see cref="Envelope.DeliverAt"/>.</summary>
    private readonly List<Envelope> _delayed = [];

    private readonly Lock _delayedGate = new();

    public string Name { get; } = name;

    /// <summary>Глубина очереди — основа метрики <c>avtobus.queue.depth</c> (идея 94).</summary>
    public int Depth => _channel.Reader.Count + DelayedCount;

    public int DelayedCount
    {
        get
        {
            lock (_delayedGate)
                return _delayed.Count;
        }
    }

    public async ValueTask EnqueueAsync(Envelope envelope, CancellationToken ct)
    {
        var now = time.GetUtcNow();

        // Отложенная доставка: сообщение придерживается до срока и не мешает остальным.
        if (!envelope.IsDue(now))
        {
            lock (_delayedGate)
                _delayed.Add(envelope);
            return;
        }

        await _channel.Writer.WriteAsync(envelope, ct).ConfigureAwait(false);
    }

    /// <summary>Перекладывает созревшие отложенные сообщения в основной канал.</summary>
    public async ValueTask PromoteDueAsync(CancellationToken ct)
    {
        var now = time.GetUtcNow();
        List<Envelope>? due = null;

        lock (_delayedGate)
        {
            for (var i = _delayed.Count - 1; i >= 0; i--)
            {
                if (!_delayed[i].IsDue(now))
                    continue;

                (due ??= []).Add(_delayed[i]);
                _delayed.RemoveAt(i);
            }
        }

        if (due is null)
            return;

        foreach (var envelope in due)
            await _channel.Writer.WriteAsync(envelope, ct).ConfigureAwait(false);
    }

    public IAsyncEnumerable<Envelope> ReadAllAsync(CancellationToken ct)
        => _channel.Reader.ReadAllAsync(ct);

    public void Complete() => _channel.Writer.TryComplete();
}
