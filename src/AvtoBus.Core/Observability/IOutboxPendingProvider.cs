namespace AvtoBus.Observability;

/// <summary>
/// Источник метрики <c>avtobus.outbox.pending</c> (идея 302): количество сообщений
/// в transactional outbox, ещё не отправленных в транспорт. Реализуется outbox-релеем.
/// Poll-only: счётчик читается наблюдателями при сборе метрик.
/// </summary>
public interface IOutboxPendingProvider
{
    /// <summary>Текущее количество ожидающих отправки outbox-сообщений.</summary>
    long OutboxPending { get; }
}
