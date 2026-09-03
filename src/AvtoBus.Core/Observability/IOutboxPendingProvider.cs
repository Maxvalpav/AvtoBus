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

/// <summary>
/// Расширенное здоровье outbox (аудит A3/E1): помимо счётчика — возраст старейшего
/// ожидающего сообщения. Именно возраст отличает «много сообщений» от «застряло».
/// </summary>
public interface IOutboxHealthProvider : IOutboxPendingProvider
{
    /// <summary>
    /// Момент создания (UTC) старейшего неотправленного сообщения или null, если очередь пуста.
    /// </summary>
    DateTime? OldestPendingAt { get; }
}
