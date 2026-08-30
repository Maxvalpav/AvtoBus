using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using AvtoBus.Runtime;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Relay: читает outbox-строки из БД (claim через SKIP LOCKED) и отправляет конверты в транспорт.
/// Push через signal + polling fallback (док 15, §4).
/// </summary>
public sealed class OutboxRelay : BackgroundService, AvtoBus.Observability.IOutboxPendingProvider
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TransportRegistry _transports;
    private readonly IEnvelopeSerializer _ser;
    private readonly IOutboxSignal _signal;
    private readonly OutboxOptions _opt;
    private readonly ILogger<OutboxRelay> _log;
    private readonly string _claimBy = $"{Environment.MachineName}/{Environment.ProcessId}";
    private long _pending;

    public OutboxRelay(
        IServiceScopeFactory scopes,
        TransportRegistry transports,
        IEnvelopeSerializer ser,
        IOutboxSignal signal,
        OutboxOptions opt,
        ILogger<OutboxRelay> log)
    {
        _scopes = scopes;
        _transports = transports;
        _ser = ser;
        _signal = signal;
        _opt = opt;
        _log = log;
    }

    /// <summary>Прокси текущего количества сообщений: count(pending) по последнему pump-срезу.</summary>
    public long OutboxPending => Interlocked.Read(ref _pending);

    protected override async Task ExecuteAsync(CancellationToken stopping)
    {
        while (!stopping.IsCancellationRequested)
        {
            int pumped;
            try
            {
                pumped = await PumpAsync(stopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                return;
            }

            if (pumped == 0)
            {
                try
                {
                    await _signal.WaitAsync(_opt.PollInterval, stopping).ConfigureAwait(false);
                }
                catch { return; }
            }
        }
    }

    private async Task<int> PumpAsync(CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();

        await using var tx = await db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

        // claim: FOR UPDATE SKIP LOCKED в транзакции — каждую строку может взять только один relay.
        // ClaimedAt старше StaleClaim считается осиротевшим (relay умер после claim, до отправки) и пере-claim'ится.
        var staleBefore = DateTime.UtcNow - _opt.StaleClaim;
        var claimed = await db.Set<OutboxMessage>()
            .FromSqlInterpolated($"""
                SELECT * FROM avtobus_outbox
                WHERE "SentAt" IS NULL
                  AND ("ClaimedAt" IS NULL OR "ClaimedAt" <= {staleBefore})
                  AND ("SendAfter" IS NULL OR "SendAfter" <= {DateTime.UtcNow})
                ORDER BY "Id"
                LIMIT {_opt.BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(ct).ConfigureAwait(false);

        if (claimed.Count == 0)
        {
            await tx.CommitAsync(ct).ConfigureAwait(false);
            return 0;
        }

        Interlocked.Add(ref _pending, claimed.Count);

        foreach (var m in claimed)
        {
            m.ClaimedAt = DateTime.UtcNow;
            m.ClaimedBy = _claimBy;
        }

        await db.SaveChangesAsync(ct).ConfigureAwait(false);
        await tx.CommitAsync(ct).ConfigureAwait(false);

        var sent = new List<long>(claimed.Count);
        var failed = new List<(long Id, string Error)>();

        await Parallel.ForEachAsync(
            claimed.GroupBy(m => m.PartitionKey ?? m.MessageId.ToString()),
            new ParallelOptions { MaxDegreeOfParallelism = _opt.Parallelism, CancellationToken = ct },
            async (group, token) =>
            {
                foreach (var m in group)
                {
                    try
                    {
                        var env = _ser.Deserialize(m.EnvelopeBlob);
                        var transport = _transports.Get(m.Transport.Length == 0 ? null : m.Transport);
                        await transport.SendAsync(env, TransportDestination.Queue(m.Destination), token).ConfigureAwait(false);
                        lock (sent) sent.Add(m.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Outbox-отправка не удалась для {MessageId}", m.MessageId);
                        lock (failed) failed.Add((m.Id, ex.Message));
                        break;
                    }
                }
            });

        if (sent.Count > 0)
        {
            Interlocked.Add(ref _pending, -sent.Count);

            // Маркировка — идемпотентная фиксация уже отправленного факта: завершаем её даже при
            // остановке, иначе сообщение «отправлено, но SentAt не проставлен» вернётся дублем.
            await db.Set<OutboxMessage>()
                .Where(o => sent.Contains(o.Id))
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.SentAt, DateTime.UtcNow)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), CancellationToken.None)
                .ConfigureAwait(false);
        }

        foreach (var (id, err) in failed)
        {
            await db.Set<OutboxMessage>()
                .Where(o => o.Id == id)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.Attempt, o => o.Attempt + 1)
                    .SetProperty(o => o.LastError, err)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), CancellationToken.None)
                .ConfigureAwait(false);
        }

        return claimed.Count;
    }
}
