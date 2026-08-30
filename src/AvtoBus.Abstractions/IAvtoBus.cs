namespace AvtoBus.Abstractions;

public interface IAvtoBus
{
    ValueTask SendAsync(object command, CancellationToken ct = default);

    ValueTask PublishAsync(object @event, CancellationToken ct = default);

    ValueTask<TReply> InvokeAsync<TReply>(object message, CancellationToken ct = default);

    ValueTask ScheduleAsync(object message, TimeSpan delay, CancellationToken ct = default);
    ValueTask ScheduleAtAsync(object message, DateTimeOffset at, CancellationToken ct = default);
}
