using Microsoft.EntityFrameworkCore.Diagnostics;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// После коммита, в котором появились outbox-строки, будит relay. Replay-безопасно:
/// только факт наличия изменения триггерит сигнал, а не каждая строка.
/// </summary>
public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IOutboxSignal _signal;

    public OutboxSaveChangesInterceptor(IOutboxSignal signal) => _signal = signal;

    public override ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData, int result, CancellationToken ct = default)
    {
        if (eventData.Context is not null &&
            eventData.Context.ChangeTracker.Entries<OutboxMessage>().Any())
        {
            _signal.Nudge();
        }

        return base.SavedChangesAsync(eventData, result, ct);
    }
}
