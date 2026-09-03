namespace AvtoBus.Abstractions;

/// <summary>
/// Исторический дубль шины уровня абстракций: никем не реализован и не используется
/// (ядро работает через <c>AvtoBus.IBus</c>). Оставлен для совместимости,
/// новые интеграции — только на <c>IBus</c>.
/// </summary>
[Obsolete("Используйте AvtoBus.IBus — IAvtoBus никем не реализован и будет удалён в 1.0.")]
public interface IAvtoBus
{
    ValueTask SendAsync(object command, CancellationToken ct = default);

    ValueTask PublishAsync(object @event, CancellationToken ct = default);

    ValueTask<TReply> InvokeAsync<TReply>(object message, CancellationToken ct = default);

    ValueTask ScheduleAsync(object message, TimeSpan delay, CancellationToken ct = default);
    ValueTask ScheduleAtAsync(object message, DateTimeOffset at, CancellationToken ct = default);
}
