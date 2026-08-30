using System.Threading.Channels;

namespace AvtoBus.InMemory;

/// <summary>
/// Одна очередь in-memory брокера. Priority queue + WFQ (идея 1).
/// Bounded по емкости; при переполнении паблишер ждет. Приоритет 0-10 (10 первым), внутри одного приоритета FIFO.
/// WFQ: вес тенанта учитывается как `priority + tenantWeight` (тонкая настройка через header `avtobus.wfq-weight`).
/// Back-pressure как у Channel (BoundedChannelFullMode.Wait).
/// </summary>
internal sealed class InMemoryQueue(string name, int capacity, TimeProvider time)
{
    private readonly System.Collections.Generic.PriorityQueue<PendingMessage, (int priority, long seq)> _pq = new(new PriorityComparer());
    private long _seq;
    private readonly SemaphoreSlim _signal = new(0);
    private readonly Lock _gate = new();
    private readonly List<PendingMessage> _delayed = [];
    private readonly Lock _delayedGate = new();

    public string Name { get; } = name;

    public int Depth
    {
        get { lock (_gate) return _pq.Count; }
    }

    public int DelayedCount
    {
        get { lock (_delayedGate) return _delayed.Count; }
    }

    public async ValueTask EnqueueAsync(Envelope envelope, CancellationToken ct)
    {
        var now = time.GetUtcNow();
        if (!envelope.IsDue(now))
        {
            lock (_delayedGate) _delayed.Add(new PendingMessage(envelope));
            return;
        }
        // Bounded wait: если переполнение — ждем пока место освободится
        while (true)
        {
            lock (_gate)
            {
                if (_pq.Count < capacity)
                {
                    var prio = -envelope.Priority; // PriorityQueue — min-heap, инвертируем
                    var seq = Interlocked.Increment(ref _seq);
                    // WFQ weight: tenant weight добавляет к приоритету (отрицательно — выше приоритет)
                    if (envelope.Headers.TryGetValue("avtobus.wfq-weight", out var w) && int.TryParse(w, out var weight))
                        prio -= weight;
                    _pq.Enqueue(new PendingMessage(envelope), (prio, seq));
                    _signal.Release();
                    return;
                }
            }
            await Task.Delay(10, ct);
        }
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
        foreach (var m in due) await EnqueueAsync(m.Envelope, ct);
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
            await _signal.WaitAsync(ct);
            PendingMessage msg;
            lock (_gate) msg = _pq.Dequeue();
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
                return true;
            }
        }
        message = default;
        return false;
    }

    public ValueTask<bool> WaitToReadAsync(CancellationToken ct) => new(_signal.WaitAsync(ct).IsCompletedSuccessfully);

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
