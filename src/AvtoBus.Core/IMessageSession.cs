namespace AvtoBus;

/// <summary>
/// Scoped-сессия отправки, связанная с текущим Unit of Work (ADR-0002).
/// В отличие от singleton <see cref="IBus"/>, сообщения сессии уходят в транзакционный outbox,
/// если он подключён (см. <see cref="IOutboxSink"/>): запись становится атомарной с бизнес-данными
/// в той же транзакции. Без outbox сессия отправляет сразу в транспорт.
///
/// Сценарии:
/// - HTTP-endpoint: внедрить <c>IMessageSession</c> в контроллер, вызвать SendAsync/PublishAsync,
///   затем сохранить бизнес-данные через DbContext — сообщение зафиксируется только вместе с коммитом.
/// - Handler: объявить параметром <c>IMessageSession session</c> — генератор разрешит его из DI скоупа.
///
/// Гарантия атомарности: сообщение не доставляется, пока не зафиксирована транзакция, в которой
/// была записана outbox-строка. Хендлер, не вызвавший SaveChanges, «не отправит» сообщение —
/// это цена атомарности: без коммита нет публикации.
/// </summary>
public interface IMessageSession
{
    /// <summary>Команда: отправить через текущий UoW (outbox при подключении, иначе транспорт).</summary>
    ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default)
        where T : class;

    /// <summary>Событие: опубликовать через текущий UoW (outbox при подключении, иначе транспорт).</summary>
    ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default)
        where T : class;
}

/// <summary>
/// Синк транзакционного outbox: <see cref="IMessageSession"/> отдаёт ему исходящие сообщения
/// вместо немедленной отправки в транспорт. Реализуется провайдерным пакетом
/// (например, <c>AvtoBus.Outbox.EfCore</c>); Core лишь вызывает его, если он зарегистрирован в скоупе.
/// </summary>
public interface IOutboxSink
{
    /// <summary>Записывает конверт в outbox текущей (открытой) транзакции.</summary>
    ValueTask EnqueueAsync(Envelope envelope, string destination, string? transport, CancellationToken ct);
}
