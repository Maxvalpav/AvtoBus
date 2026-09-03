using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Outbox.EfCore;

/// <summary>Куда адресован исходящий конверт (док 15, §2).</summary>
public readonly record struct OutboxRoute(string Destination, string? Transport);

/// <summary>Транзакционный outbox: сообщение пишется в БД в одной транзакции с бизнес-данными.</summary>
public interface IOutbox
{
    ValueTask EnqueueAsync(Envelope env, OutboxRoute route, CancellationToken ct);
}

/// <summary>EF-реализация: складывает конверт в <c>avtobus_outbox</c> и будит relay после коммита.</summary>
public sealed class EfCoreOutbox<TDbContext> : IOutbox, IOutboxSink, IDisposable where TDbContext : DbContext
{
    private readonly TDbContext _db;
    private readonly IEnvelopeSerializer _ser;
    private readonly IOutboxSignal _signal;
    private readonly TimeProvider _clock;
    private readonly EventHandler<SavedChangesEventArgs> _savedChangesHandler;
    private int _disposed;

    public EfCoreOutbox(TDbContext db, IEnvelopeSerializer ser, IOutboxSignal signal, TimeProvider clock)
    {
        _db = db;
        _ser = ser;
        _signal = signal;
        _clock = clock;
        // Храним делегат, чтобы отписаться в Dispose: иначе при DbContext pooling
        // подписка переживает возврат контекста в пул (утечка + многократный Nudge).
        _savedChangesHandler = (_, _) => _signal.Nudge();
        _db.SavedChanges += _savedChangesHandler;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        _db.SavedChanges -= _savedChangesHandler;
    }

    public async ValueTask EnqueueAsync(Envelope env, OutboxRoute route, CancellationToken ct)
    {
        _db.Set<OutboxMessage>().Add(new OutboxMessage
        {
            MessageId = env.MessageId,
            Destination = route.Destination,
            Transport = route.Transport ?? "",
            MessageType = env.MessageType,
            PartitionKey = env.PartitionKey,
            TenantId = env.TenantId,
            EnvelopeBlob = _ser.Serialize(env),
            CreatedAt = _clock.GetUtcNow().UtcDateTime,
            SendAfter = env.DeliverAt?.UtcDateTime,
        });

        await Task.CompletedTask;
    }

    /// <summary>
    /// Адаптер под <see cref="IOutboxSink"/> (ADR-0002): <see cref="IMessageSession"/> отдаёт
    /// сюда сообщения, чтобы они стали outbox-строками текущей транзакции.
    /// </summary>
    public ValueTask EnqueueAsync(Envelope env, string destination, string? transport, CancellationToken ct)
        => EnqueueAsync(env, new OutboxRoute(destination, transport), ct);
}
