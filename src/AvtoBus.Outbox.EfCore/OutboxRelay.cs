using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using AvtoBus;
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
            catch (Exception ex)
            {
                // Раньше любое исключение убивало BackgroundService навсегда (silent stall outbox).
                // Логируем и повторяем через интервал вместо смерти relay.
                _log.LogError(ex, "Outbox pump failed, retrying");
                try { await Task.Delay(_opt.PollInterval, stopping).ConfigureAwait(false); }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested) { return; }
                continue;
            }

            if (pumped == 0)
            {
                try
                {
                    await _signal.WaitAsync(_opt.PollInterval, stopping).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "Outbox signal wait failed");
                    try { await Task.Delay(_opt.PollInterval, stopping).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (stopping.IsCancellationRequested) { return; }
                }
            }
        }
    }

    private async Task<int> PumpAsync(CancellationToken ct)
    {
        List<OutboxMessage> claimed;
        // Claim-скоуп живёт только на время claim+commit: DbContext/транзакция
        // не держатся через сетевые SendAsync (иначе пул соединений умирает на медленном брокере).
        await using (var scope = _scopes.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<DbContext>();

            await using var tx = await db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

            // claim: FOR UPDATE SKIP LOCKED в транзакции — каждую строку может взять только один relay.
            // ClaimedAt старше StaleClaim считается осиротевшим (relay умер после claim, до отправки) и пере-claim'ится.
            var staleBefore = DateTime.UtcNow - _opt.StaleClaim;
            claimed = await db.Set<OutboxMessage>()
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
        }

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
                        // Вид назначения хранится в строке: топик идёт в fan-out,
                        // а не в одноимённую очередь (иначе подписчики ничего не получали).
                        var destination = m.Kind == (int)DestinationKind.Topic
                            ? TransportDestination.Topic(m.Destination)
                            : TransportDestination.Queue(m.Destination);
                        await transport.SendAsync(env, destination, token).ConfigureAwait(false);
                        lock (sent) sent.Add(m.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Outbox-отправка не удалась для {MessageId}", m.MessageId);
                        lock (failed) failed.Add((m.Id, ex.Message));
                        continue;
                    }
                }
            });

        if (sent.Count > 0 || failed.Count > 0)
        {
            Interlocked.Add(ref _pending, -(sent.Count + failed.Count));

            await using var markScope = _scopes.CreateAsyncScope();
            var markDb = markScope.ServiceProvider.GetRequiredService<DbContext>();

            // Маркировка — идемпотентная фиксация уже отправленного факта: завершаем её даже при
            // остановке, иначе сообщение «отправлено, но SentAt не проставлен» вернётся дублем.
            await markDb.Set<OutboxMessage>()
                .Where(o => sent.Contains(o.Id))
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.SentAt, DateTime.UtcNow)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), CancellationToken.None)
                .ConfigureAwait(false);
        }

        if (failed.Count > 0)
        {
            // Индексер словаря failedMap[o.Id] НЕ транслируется EF в SQL (проверено на PG:
            // InvalidOperationException при первой же неудаче — pump вставал навсегда).
            // Поэтому один raw UPDATE с CASE: задержки считаем на клиенте (cap+jitter).
            var failedIds = new HashSet<long>(failed.Select(f => f.Id));
            var attempts = claimed.Where(m => failedIds.Contains(m.Id))
                .ToDictionary(m => m.Id, m => m.Attempt);
            await using var failScope = _scopes.CreateAsyncScope();
            var failDb = failScope.ServiceProvider.GetRequiredService<DbContext>();
            await MarkFailedAsync(failDb, failed, attempts, CancellationToken.None).ConfigureAwait(false);
        }

        return claimed.Count;
    }

    /// <summary>
    /// Batch-mark failed: Attempt+1, per-row LastError и SendAfter (2^Attempt с cap 1ч + jitter ±20%).
    /// Один roundtrip вместо N обновлений; CASE обходит нетранслируемость словаря в EF.
    /// </summary>
    internal static async Task MarkFailedAsync(
        DbContext db,
        IReadOnlyCollection<(long Id, string Error)> failed,
        IReadOnlyDictionary<long, int> attempts,
        CancellationToken ct)
    {
        var sql = new System.Text.StringBuilder(
            "UPDATE avtobus_outbox SET \"Attempt\" = \"Attempt\" + 1, \"ClaimedAt\" = NULL, \"SendAfter\" = CASE \"Id\" ");
        var err = new System.Text.StringBuilder("CASE \"Id\" ");
        var ids = new System.Text.StringBuilder();

        // Параметры провайдер-нейтральные (без зависимости на Npgsql): через фабрику соединения.
        var factory = System.Data.Common.DbProviderFactories.GetFactory(db.Database.GetDbConnection())
            ?? throw new InvalidOperationException("Провайдер БД не найден.");
        var pars = new List<object>();
        void AddParam(string name, object value)
        {
            System.Data.Common.DbParameter p = factory.CreateParameter()
                ?? throw new InvalidOperationException("Провайдер не создал параметр.");
            p.ParameterName = name;
            p.Value = value;
            pars.Add(p);
        }

        var i = 0;
        foreach (var (id, error) in failed)
        {
            attempts.TryGetValue(id, out var attempt);
            // Было 2^Attempt без cap — Attempt=20 давал ~12 суток и переполнение.
            var delaySeconds = Math.Min(Math.Pow(2, Math.Min(attempt, 10)) * (0.8 + Random.Shared.NextDouble() * 0.4), 3600);
            var sendAfter = DateTime.UtcNow.AddSeconds(delaySeconds);
            sql.Append($"WHEN @id{i} THEN @sa{i} ");
            err.Append($"WHEN @id{i} THEN @err{i} ");
            ids.Append(i == 0 ? $"@id{i}" : $", @id{i}");
            AddParam($"@id{i}", id);
            AddParam($"@sa{i}", sendAfter);
            AddParam($"@err{i}", error);
            i++;
        }
        sql.Append("ELSE \"SendAfter\" END, \"LastError\" = ").Append(err).Append("ELSE \"LastError\" END WHERE \"Id\" IN (").Append(ids).Append(')');

        await db.Database.ExecuteSqlRawAsync(sql.ToString(), pars.ToArray(), ct).ConfigureAwait(false);
    }
}
