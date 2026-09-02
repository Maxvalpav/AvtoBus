using System.Threading.Channels;

namespace AvtoBus.InMemory;

/// <summary>
/// Одна очередь in-memory брокера. Priority queue + WFQ (идея 1).
/// Bounded по емкости; при переполнении паблишер ждет. Приоритет 0-10 (10 первым), внутри одного приоритета FIFO.
/// WFQ: вес тенанта учитывается как `priority + tenantWeight` (тонкая настройка через header `avtobus.wfq-weight`).
/// Back-pressure как у Channel (BoundedChannelFullMode.Wait).
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly string _name;
    private readonly int _capacity;
    private readonly TimeProvider _time;
    private readonly System.Collections.Generic.PriorityQueue<PendingMessage, (int priority, long seq)> _pq = new(new PriorityComparer());
    private long _seq;
    private readonly SemaphoreSlim _signal = new(0);
    private readonly SemaphoreSlim _capacityGate;
    private readonly Lock _gate = new();
    private readonly List<PendingMessage> _delayed = [];
    private readonly Lock _delayedGate = new();

    public InMemoryQueue(string name, int capacity, TimeProvider time)
    {
        _name = name;
        _capacity = capacity;
        _time = time;
        _capacityGate = new SemaphoreSlim(capacity, capacity);
    }

    public string Name => _name;
    private int capacity => _capacity;
    private TimeProvider time => _time;

    public int Depth
    {
        get { lock (_gate) return _pq.Count; }
    }

    public int DelayedCount
    {
        get { lock (_delayedGate) return _delayed.Count; }
    }

    private const int MaxDelayed = 10_000;

    public async ValueTask EnqueueAsync(Envelope envelope, CancellationToken ct)
    {
        var now = time.GetUtcNow();
        if (!envelope.IsDue(now))
        {
            lock (_delayedGate)
            {
                if (_delayed.Count >= MaxDelayed)
                    throw new InvalidOperationException($"Delayed queue '{_name}' overflow: {MaxDelayed} pending.");
                _delayed.Add(new PendingMessage(envelope));
            }
            return;
        }
        await _capacityGate.WaitAsync(ct).ConfigureAwait(false);
        lock (_gate)
        {
            var prio = -envelope.Priority;
            var seq = Interlocked.Increment(ref _seq);
            if (envelope.Headers.TryGetValue("avtobus.wfq-weight", out var w) && int.TryParse(w, out var weight))
                prio -= Math.Clamp(weight, -10, 10);
            _pq.Enqueue(new PendingMessage(envelope), (prio, seq));
        }
        _signal.Release();
    }

    public async ValueTask PromoteDueAsync(CancellationToken ct)
    {
        var now = time.GetUtcNow();
        List<PendingMessage>? due = null;
        lock (_delayedGate)
        {
            for (var i = _delayed.Count - 1; i >= 0; i--)
            {
                if (!_delayed[i].Envelope.IsDue(now)) continue;
                (due ??= []).Add(_delayed[i]);
                _delayed.RemoveAt(i);
            }
        }
        if (due is null) return;
        foreach (var m in due)
        {
            // Если очередь полна, возвращаем в delayed чтобы не блокировать поток промоушена
            if (!_capacityGate.Wait(0)) { lock (_delayedGate) _delayed.Add(m); continue; }
            try
            {
                lock (_gate)
                {
                    var prio = -m.Envelope.Priority;
                    var seq = Interlocked.Increment(ref _seq);
                    if (m.Envelope.Headers.TryGetValue("avtobus.wfq-weight", out var w) && int.TryParse(w, out var weight))
                        prio -= Math.Clamp(weight, -10, 10);
                    _pq.Enqueue(m, (prio, seq));
                }
                _signal.Release();
            }
            catch { _capacityGate.Release(); throw; }
        }
    }

    public bool CancelDelayed(Guid messageId)
    {
        lock (_delayedGate)
        {
            var idx = _delayed.FindIndex(m => m.Envelope.MessageId == messageId);
            if (idx < 0) return false;
            _delayed.RemoveAt(idx);
            return true;
        }
    }

    public async IAsyncEnumerable<PendingMessage> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await _signal.WaitAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { yield break; }
            PendingMessage msg;
            lock (_gate)
            {
                if (_pq.Count == 0)
                    continue;
                msg = _pq.Dequeue();
            }
            _capacityGate.Release();
            yield return msg;
        }
    }

    public bool TryRead(out PendingMessage message)
    {
        lock (_gate)
        {
            if (_pq.TryDequeue(out var m, out _))
            {
                message = m;
                _capacityGate.Release();
                return true;
            }
        }
        message = default;
        return false;
    }

    public async ValueTask<bool> WaitToReadAsync(CancellationToken ct)
    {
        await _signal.WaitAsync(ct).ConfigureAwait(false);
        return true;
    }

    public void Complete() { }

    private sealed class PriorityComparer : IComparer<(int priority, long seq)>
    {
        public int Compare((int priority, long seq) x, (int priority, long seq) y)
        {
            var c = x.priority.CompareTo(y.priority);
            return c != 0 ? c : x.seq.CompareTo(y.seq);
        }
    }
}

internal readonly record struct PendingMessage(Envelope Envelope);
