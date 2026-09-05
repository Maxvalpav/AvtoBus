namespace AvtoBus.Runtime;

/// <summary>
/// Хранилище inbox-дедупликации без привязки к реляционной БД (аудит 04 §1.2):
/// повторная доставка того же <c>MessageId</c> тому же консьюмеру в окне
/// подтверждается без повторной обработки. Транзакционный инвариант
/// «бизнес + inbox в одном коммите» остаётся за EF-мидлварью
/// (<c>InboxDedupMiddleware</c>); стор — best-effort дедуп для конфигураций
/// без реляционной БД (монолит, Redis-инфра).
/// </summary>
public interface IInboxStore
{
    /// <summary>
    /// Помечает сообщение обработанным. Возвращает <c>true</c> — обрабатывать,
    /// <c>false</c> — дубликат в окне, пропустить.
    /// </summary>
    ValueTask<bool> TryMarkProcessedAsync(Guid messageId, string consumer, CancellationToken ct = default);

    /// <summary>
    /// Снимает отметку: обработка не завершилась (отмена/падение до решения),
    /// ретрай не должен считаться дубликатом.
    /// </summary>
    void Forget(Guid messageId, string consumer);
}

/// <summary>
/// In-memory стор поверх <see cref="InboxDeduplication"/>: источник истины
/// для одного процесса, окно + bounded-память (500k записей, evict oldest 10%).
/// Для мульти-инстанс — <c>RedisInboxStore</c> или транзакционный EF-inbox.
/// </summary>
public sealed class InMemoryInboxStore(TimeSpan window, TimeProvider time) : IInboxStore
{
    private readonly InboxDeduplication _inner = new(window, time);

    public ValueTask<bool> TryMarkProcessedAsync(Guid messageId, string consumer, CancellationToken ct = default)
        => ValueTask.FromResult(_inner.TryMarkProcessed(messageId, consumer));

    public void Forget(Guid messageId, string consumer) => _inner.Forget(messageId, consumer);

    public int Count => _inner.Count;
}
