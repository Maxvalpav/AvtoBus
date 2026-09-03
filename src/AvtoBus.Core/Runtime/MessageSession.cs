using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// Scoped-реализация <see cref="IMessageSession"/>: маршрутизирует и сериализует сообщение
/// через тот же путь, что и <see cref="AvtoBusClient"/>, но вместо немедленной отправки в транспорт
/// (когда подключён транзакционный outbox) кладёт конверт в outbox-строку текущего Unit of Work.
///
/// Поведение:
/// - outbox подключён (в скоупе есть <see cref="IOutboxSink"/>) — конверт записывается в outbox;
///   реальная доставка происходит relay-ом после коммита транзакции. Rollback отменяет публикацию.
/// - outbox не подключён — сообщение отправляется в транспорт немедленно (как <see cref="IBus"/>).
/// </summary>
public sealed class MessageSession(AvtoBusClient bus, IServiceProvider services) : IMessageSession
{
    private IOutboxSink? _sink;
    private bool _sinkResolved;

    public ValueTask SendAsync<T>(T command, SendOptions? options = null, CancellationToken ct = default)
        where T : class
        => RouteAsync(command, OutgoingKind.Send, options, ct);

    public ValueTask PublishAsync<T>(T @event, PublishOptions? options = null, CancellationToken ct = default)
        where T : class
        => RouteAsync(@event, OutgoingKind.Publish, options, ct);

    private async ValueTask RouteAsync(object message, OutgoingKind kind, MessageOptions? options, CancellationToken ct)
    {
        var sink = GetSink();
        if (sink is null)
        {
            // Outbox не подключён: честная немедленная отправка, как у IBus.
            await bus.DispatchAsync(message, message.GetType(), kind, options, parent: null, ct).ConfigureAwait(false);
            return;
        }

        // Конверт строится тем же путём (маршрутизация, изоляция тенантов, регионы, безопасность),
        // но уходит в outbox текущей транзакции, а не в транспорт.
        var (envelope, destination, transportName, activity) = await bus.PrepareAsync(message, message.GetType(), kind, options, parent: null, ct).ConfigureAwait(false);
        using var _ = activity;
        await sink.EnqueueAsync(envelope, destination.Name, transportName, destination.Kind, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Синк резолвится лениво и один раз: не создаём DbContext в скоупах, где сессия
    /// только внедрена, но ни одного сообщения не отправила.
    /// </summary>
    private IOutboxSink? GetSink()
    {
        if (_sinkResolved)
            return _sink;

        _sink = services.GetService<IOutboxSink>();
        _sinkResolved = true;
        return _sink;
    }
}
