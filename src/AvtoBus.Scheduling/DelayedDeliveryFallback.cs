using AvtoBus.Runtime;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Scheduling;

/// <summary>
/// Единая отложенная доставка для транспортов без нативной (аудит 04 §1.1):
/// ретрай с задержкой и <c>DeferAsync</c> персистятся в <see cref="IScheduleStore"/>
/// и возвращаются в транспорт по наступлении срока штатным
/// <see cref="SchedulerService"/> (тот же claim → send → mark-delivered цикл,
/// что и у <c>ScheduleAsync</c>). Работает и с in-memory, и с EF-стором —
/// переживает рестарт вместе со стором.
/// </summary>
public sealed class DelayedDeliveryFallback(
    IScheduleStore store,
    IEnvelopeFactory envelopes,
    TimeProvider clock,
    ILogger<DelayedDeliveryFallback> log) : IDelayedDeliveryFallback
{
    public async ValueTask<bool> TryDeferAsync(
        Envelope envelope,
        TransportDestination destination,
        string? transportName,
        TimeSpan delay,
        CancellationToken ct = default)
    {
        if (delay <= TimeSpan.Zero)
            return false;

        var now = clock.GetUtcNow();
        var due = now + delay;

        try
        {
            await store.ScheduleAsync(new ScheduledMessage
            {
                Token = Guid.NewGuid(),
                MessageType = envelope.MessageType,
                EnvelopeBlob = envelopes.Serialize(envelope with { DeliverAt = due }),
                Destination = destination.Name,
                Transport = transportName ?? "",
                DeliverAt = due.UtcDateTime,
                CreatedAt = now.UtcDateTime,
                TenantId = envelope.TenantId,
            }, ct).ConfigureAwait(false);

            log.LogDebug(
                "Отложенный ретрай {MessageId} ({MessageType}) в стор до {Due} (транспорт {Transport})",
                envelope.MessageId, envelope.MessageType, due, transportName);
            return true;
        }
        catch (Exception ex)
        {
            log.LogError(ex,
                "Schedule-стор недоступен — отложенный ретрай {MessageId} не принят",
                envelope.MessageId);
            return false;
        }
    }
}
