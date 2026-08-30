using AvtoBus;
using AvtoBus.Outbox.EfCore;

namespace AvtoBus.Scheduling;

/// <summary>
/// Публичный API durable-отложенной доставки: планирует доставку сообщения в хранилище,
/// чтобы SchedulerService доставил её даже после рестарта (идеи 226, 46).
/// </summary>
public interface IScheduler
{
    ValueTask<ScheduledToken> ScheduleAsync<T>(
        T message, DateTimeOffset at,
        string destination, string? transport = null, string? uniqueKey = null,
        CancellationToken ct = default) where T : class;

    ValueTask CancelAsync(ScheduledToken token, CancellationToken ct = default);
}

internal sealed class DurableScheduler(
    IScheduleStore store,
    IEnvelopeFactory envelopes,
    TimeProvider clock) : IScheduler
{
    public async ValueTask<ScheduledToken> ScheduleAsync<T>(
        T message, DateTimeOffset at,
        string destination, string? transport = null, string? uniqueKey = null,
        CancellationToken ct = default) where T : class
    {
        var envelope = envelopes.Create(message, typeof(T), options: null, parent: null);
        var token = await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            MessageType = envelope.MessageType,
            EnvelopeBlob = envelopes.Serialize(envelope),
            Destination = destination,
            Transport = transport ?? "",
            DeliverAt = at.UtcDateTime,
            CreatedAt = clock.GetUtcNow().UtcDateTime,
            UniqueKey = uniqueKey,
        }, ct).ConfigureAwait(false);

        return new ScheduledToken(token);
    }

    public ValueTask CancelAsync(ScheduledToken token, CancellationToken ct = default)
        => store.CancelAsync(token.Value, ct);
}
