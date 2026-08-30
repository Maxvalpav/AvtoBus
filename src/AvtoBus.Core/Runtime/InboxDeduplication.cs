using System.Collections.Concurrent;

namespace AvtoBus.Runtime;

/// <summary>
/// Дедупликация входящих по (MessageId, консьюмер) в скользящем окне (идея 156).
///
/// In-memory реализация — источник истины для одного процесса. В распределённой конфигурации
/// её место занимает inbox-таблица в той же транзакции, что и бизнес-изменения.
/// </summary>
public sealed class InboxDeduplication(TimeSpan window, TimeProvider time)
{
    private readonly ConcurrentDictionary<(Guid, string), DateTimeOffset> _seen = new();
    private DateTimeOffset _lastSweep;
    private readonly Lock _sweepGate = new();

    /// <summary>
    /// Отмечает сообщение обработанным. <c>false</c> означает «уже видели» — дубликат.
    /// </summary>
    public bool TryMarkProcessed(Guid messageId, string consumer)
    {
        var now = time.GetUtcNow();
        SweepIfDue(now);

        return _seen.TryAdd((messageId, consumer), now);
    }

    /// <summary>Снимает отметку: обработка провалилась, ретрай не должен считаться дубликатом.</summary>
    public void Forget(Guid messageId, string consumer) => _seen.TryRemove((messageId, consumer), out _);

    public int Count => _seen.Count;

    /// <summary>Чистит протухшие записи. Раз в окно, не на каждом сообщении.</summary>
    private void SweepIfDue(DateTimeOffset now)
    {
        if (now - _lastSweep < window)
            return;

        lock (_sweepGate)
        {
            if (now - _lastSweep < window)
                return;

            _lastSweep = now;

            foreach (var (key, seenAt) in _seen)
            {
                if (now - seenAt >= window)
                    _seen.TryRemove(key, out _);
            }
        }
    }
}
