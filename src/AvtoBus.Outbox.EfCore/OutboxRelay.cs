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
public sealed class OutboxRelay : BackgroundService,
    AvtoBus.Observability.IOutboxPendingProvider,
    AvtoBus.Observability.IOutboxHealthProvider
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TransportRegistry _transports;
    private readonly IEnvelopeSerializer _ser;
    private readonly IOutboxSignal _signal;
    private readonly OutboxOptions _opt;
    private readonly ILogger<OutboxRelay> _log;
    private readonly TimeProvider _time;
    // Владелец лиз уникален на инстанс relay, а не на процесс (аудит A1): два relay
    // в одном процессе (тесты, хосты) иначе никогда не исключают друг друга.
    private readonly string _claimBy = $"{Environment.MachineName}/{Environment.ProcessId}/{Guid.NewGuid():N}";
    private long _pending;

    /// <summary>Тики UTC старейшего неотправленного сообщения (0 — очередь пуста).</summary>
    private long _oldestPendingTicks;

    private DateTime _lastHealthRefresh = DateTime.MinValue;

    public OutboxRelay(
        IServiceScopeFactory scopes,
        TransportRegistry transports,
        IEnvelopeSerializer ser,
        IOutboxSignal signal,
        OutboxOptions opt,
        ILogger<OutboxRelay> log,
        TimeProvider? time = null)
    {
        _scopes = scopes;
        _transports = transports;
        _ser = ser;
        _signal = signal;
        _opt = opt;
        _log = log;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Прокси текущего количества сообщений: count(pending) по последнему pump-срезу.</summary>
    public long OutboxPending => Interlocked.Read(ref _pending);

    public DateTime? OldestPendingAt
    {
        get
        {
            var ticks = Interlocked.Read(ref _oldestPendingTicks);
            return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
        }
    }

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
                // На простое сверяем здоровье с БД (аудит A3): дельта в памяти врёт
                // после рестарта и в многоинстансной среде. Лёгкий запрос, не чаще интервала.
                if (_time.GetUtcNow().UtcDateTime - _lastHealthRefresh >= _opt.HealthRefreshInterval)
                {
                    try
                    {
                        await RefreshHealthAsync(stopping).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _log.LogDebug(ex, "Outbox health refresh failed");
                    }
                }

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

    /// <summary>
    /// Сверка здоровья с БД (аудит A3): реальный COUNT + возраст старейшего.
    /// Вызывается только на простое, не чаще <see cref="OutboxOptions.HealthRefreshInterval"/>.
    /// </summary>
    private async Task RefreshHealthAsync(CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();

        var health = await db.Database
            .SqlQueryRaw<PendingHealth>(
                """
                SELECT COUNT(*) AS "Count", MIN("CreatedAt") AS "Oldest"
                FROM avtobus_outbox WHERE "SentAt" IS NULL
                """)
            .SingleAsync(ct).ConfigureAwait(false);

        Interlocked.Exchange(ref _pending, health.Count);
        Interlocked.Exchange(ref _oldestPendingTicks, health.Oldest?.ToUniversalTime().Ticks ?? 0);
        _lastHealthRefresh = _time.GetUtcNow().UtcDateTime;
    }

    private sealed record PendingHealth(long Count, DateTime? Oldest);

    private async Task<int> PumpAsync(CancellationToken ct)
    {
        // Порядок фаз строгий (аудит A1): peek ключей → захват лиз → claim ТОЛЬКО
        // своих ключей и бесключевых строк. Кто не владеет ключом — тот его строк
        // даже не клеймит, поэтому обогнать владельца (skip-ahead) невозможно,
        // и снимать чужой claim не нужно.
        var ownedKeys = await AcquirePartitionLeasesAsync(ct).ConfigureAwait(false);

        List<OutboxMessage> claimed;
        // Claim-скоуп живёт только на время claim+commit: DbContext/транзакция
        // не держатся через сетевые SendAsync (иначе пул соединений умирает на медленном брокере).
        await using (var scope = _scopes.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<DbContext>();

            await using var tx = await db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

            // claim: FOR UPDATE SKIP LOCKED в транзакции — каждую строку может взять только один relay.
            // ClaimedAt старше StaleClaim считается осиротевшим (relay умер после claim, до отправки) и пере-claim'ится.
            // Время — через TimeProvider (аудит G1): DateTime.UtcNow ломал StaleClaim при рассинхроне часов.
            var now = _time.GetUtcNow().UtcDateTime;
            var staleBefore = now - _opt.StaleClaim;
            claimed = await ClaimAsync(db, ownedKeys, _opt.BatchSize, now, staleBefore, ct).ConfigureAwait(false);

            if (claimed.Count == 0)
            {
                await tx.CommitAsync(ct).ConfigureAwait(false);
                return 0;
            }

            Interlocked.Add(ref _pending, claimed.Count);

            foreach (var m in claimed)
            {
                m.ClaimedAt = now;
                m.ClaimedBy = _claimBy;
            }

            await db.SaveChangesAsync(ct).ConfigureAwait(false);
            await tx.CommitAsync(ct).ConfigureAwait(false);
        }

        var groups = claimed.GroupBy(m => m.PartitionKey).ToList();

        var sent = new List<long>(claimed.Count);
        var failed = new List<(long Id, string Error)>();
        var deferred = new List<(string Key, long Id)>();

        try
        {
            await Parallel.ForEachAsync(
                groups,
                new ParallelOptions { MaxDegreeOfParallelism = _opt.Parallelism, CancellationToken = ct },
                async (group, token) =>
                {
                    // Head-of-line внутри ключа (аудит 1.2): упало одно сообщение ключа —
                    // остаток группы ждёт вместе с головой (deferred с её backoff),
                    // иначе порядок per key нарушается.
                    // Бесключевые сообщения независимы (общий null-ключ — лишь группировка):
                    // их продолжаем по одному, иначе одна ошибка бросает весь батч
                    // в claim-limbo до StaleClaim.
                    var headOfLine = group.Key is not null;
                    var items = group.ToList();
                    for (var i = 0; i < items.Count; i++)
                    {
                        var m = items[i];
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
                            if (headOfLine)
                            {
                                lock (deferred)
                                    for (var j = i + 1; j < items.Count; j++)
                                        deferred.Add((group.Key!, items[j].Id));
                                break;
                            }
                        }
                    }
                });

            if (sent.Count > 0 || failed.Count > 0)
            {
                Interlocked.Add(ref _pending, -(sent.Count + failed.Count));

                await using var markScope = _scopes.CreateAsyncScope();
                var markDb = markScope.ServiceProvider.GetRequiredService<DbContext>();
                var markedAt = _time.GetUtcNow().UtcDateTime;

                // Маркировка — идемпотентная фиксация уже отправленного факта: завершаем её даже при
                // остановке, иначе сообщение «отправлено, но SentAt не проставлен» вернётся дублем.
                await markDb.Set<OutboxMessage>()
                    .Where(o => sent.Contains(o.Id))
                    .ExecuteUpdateAsync(s => s
                        .SetProperty(o => o.SentAt, markedAt)
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
                var sendAfters = await MarkFailedAsync(
                    failDb, failed, attempts, CancellationToken.None, _time).ConfigureAwait(false);

                // Отложенные подписчики ключа ждут вместе с головой: их SendAfter —
                // максимум backoff упавших того же ключа (без Attempt: они не падали).
                // Иначе они либо уйдут раньше головы (нарушение порядка), либо залипнут до StaleClaim.
                if (deferred.Count > 0)
                {
                    var keyById = claimed.Where(m => m.PartitionKey is not null)
                        .ToDictionary(m => m.Id, m => m.PartitionKey!);
                    var maxBackoffByKey = new Dictionary<string, DateTime>(StringComparer.Ordinal);
                    foreach (var (id, _) in failed)
                    {
                        if (keyById.TryGetValue(id, out var key)
                            && sendAfters.TryGetValue(id, out var sa)
                            && (!maxBackoffByKey.TryGetValue(key, out var cur) || sa > cur))
                            maxBackoffByKey[key] = sa;
                    }
                    foreach (var keyGroup in deferred.GroupBy(d => d.Key))
                    {
                        if (!maxBackoffByKey.TryGetValue(keyGroup.Key, out var backoff))
                            backoff = _time.GetUtcNow().UtcDateTime;
                        var ids = keyGroup.Select(d => d.Id).ToList();
                        await failDb.Set<OutboxMessage>()
                            .Where(o => ids.Contains(o.Id))
                            .ExecuteUpdateAsync(s => s.SetProperty(o => o.SendAfter, backoff), CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                }
            }
        }
        finally
        {
            // Лизу отпускаем всегда — даже при отмене: иначе ключ залипнет до TTL.
            // Безопасно: следующий pump заново возьмёт лизу перед claim своих строк.
            await ReleasePartitionsAsync(ownedKeys, CancellationToken.None).ConfigureAwait(false);
        }

        return claimed.Count;
    }

    /// <summary>
    /// Фаза 1 pump: какие ключи ждут отправки + захват/продление их лиз.
    /// Возвращает ключи, принадлежащие этому инстансу на время pump.
    /// </summary>
    private async Task<HashSet<string>> AcquirePartitionLeasesAsync(CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var now = _time.GetUtcNow().UtcDateTime;

        var pendingKeys = await db.Database
            .SqlQueryRaw<string>(
                """
                SELECT DISTINCT "PartitionKey" FROM avtobus_outbox
                WHERE "SentAt" IS NULL
                  AND ("ClaimedAt" IS NULL OR "ClaimedAt" <= {0})
                  AND ("SendAfter" IS NULL OR "SendAfter" <= {1})
                  AND "PartitionKey" IS NOT NULL
                LIMIT {2}
                """, now - _opt.StaleClaim, now, _opt.BatchSize)
            .ToListAsync(ct).ConfigureAwait(false);

        var owned = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in pendingKeys)
        {
            if (await TryAcquirePartitionLeaseAsync(db, key, _claimBy, now, _opt.PartitionLeaseTtl, ct).ConfigureAwait(false))
                owned.Add(key);
        }

        return owned;
    }

    /// <summary>
    /// Фаза 2 pump: claim строк — только свои ключи и бесключевые (аудит A1).
    /// IN-список собирается параметризованно (как в <see cref="MarkFailedAsync"/>),
    /// LIMIT инлайнится: это проверенный int (BatchSize ≥ 1 по Validate()).
    /// </summary>
    private static async Task<List<OutboxMessage>> ClaimAsync(
        DbContext db, HashSet<string> ownedKeys, int batchSize,
        DateTime now, DateTime staleBefore, CancellationToken ct)
    {
        var factory = System.Data.Common.DbProviderFactories.GetFactory(db.Database.GetDbConnection())
            ?? throw new InvalidOperationException("Провайдер БД не найден.");
        System.Data.Common.DbParameter Param(string name, object value)
        {
            var p = factory.CreateParameter()
                ?? throw new InvalidOperationException("Провайдер не создал параметр.");
            p.ParameterName = name;
            p.Value = value;
            return p;
        }

        var sql = new System.Text.StringBuilder(
            """
            SELECT * FROM avtobus_outbox
            WHERE "SentAt" IS NULL
              AND ("ClaimedAt" IS NULL OR "ClaimedAt" <= @stale)
              AND ("SendAfter" IS NULL OR "SendAfter" <= @now)
            """);
        var pars = new List<object> { Param("@stale", staleBefore), Param("@now", now) };

        if (ownedKeys.Count > 0)
        {
            var i = 0;
            var names = new List<string>(ownedKeys.Count);
            foreach (var key in ownedKeys)
            {
                var name = $"@k{i++}";
                names.Add(name);
                pars.Add(Param(name, key));
            }
            sql.Append($" AND (\"PartitionKey\" IS NULL OR \"PartitionKey\" IN ({string.Join(", ", names)}))");
        }
        else
        {
            sql.Append(" AND \"PartitionKey\" IS NULL");
        }
        sql.Append($" ORDER BY \"Id\" LIMIT {Math.Max(1, batchSize)} FOR UPDATE SKIP LOCKED");

        return await db.Set<OutboxMessage>()
            .FromSqlRaw(sql.ToString(), pars.ToArray())
            .ToListAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Атомарный захват лизы партиции: перехват просроченной либо вставка новой.
    /// PK-гонку на вставке проигравший определяет по исключению/нулю строк — false.
    /// </summary>
    public static async Task<bool> TryAcquirePartitionLeaseAsync(
        DbContext db, string partitionKey, string owner, DateTime now, TimeSpan ttl, CancellationToken ct)
    {
        var expires = now + ttl;

        var taken = await db.Database.ExecuteSqlInterpolatedAsync(
            $"""
            UPDATE avtobus_outbox_leases SET "Owner"={owner}, "ExpiresAt"={expires}
            WHERE "PartitionKey"={partitionKey} AND ("ExpiresAt"<={now} OR "Owner"={owner})
            """, ct).ConfigureAwait(false);
        if (taken == 1)
            return true;

        try
        {
            var inserted = await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                INSERT INTO avtobus_outbox_leases ("PartitionKey", "Owner", "ExpiresAt")
                SELECT {partitionKey}, {owner}, {expires}
                WHERE NOT EXISTS (SELECT 1 FROM avtobus_outbox_leases WHERE "PartitionKey"={partitionKey})
                """, ct).ConfigureAwait(false);
            return inserted == 1;
        }
        catch (Exception)
        {
            // Гонка вставки (PK-конфликт приходит не всегда как DbUpdateException —
            // провайдер может бросить сырой PostgresException) либо любая другая ошибка:
            // считаем лизу проигранной и отдаём строки владельцу следующим pump.
            // Fail-open здесь безопасен: at-least-once допускает дубли (ловит inbox-дедуп),
            // а падение pump оставляло бы строки в claim до StaleClaim.
            return false;
        }
    }

    private async Task ReleasePartitionsAsync(IReadOnlyCollection<string> leasedKeys, CancellationToken ct)
    {
        if (leasedKeys.Count == 0)
            return;

        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        foreach (var key in leasedKeys)
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                DELETE FROM avtobus_outbox_leases WHERE "PartitionKey"={key} AND "Owner"={_claimBy}
                """, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Batch-mark failed: Attempt+1, per-row LastError и SendAfter (2^Attempt с cap 1ч + jitter ±20%).
    /// Один roundtrip вместо N обновлений; CASE обходит нетранслируемость словаря в EF.
    /// Возвращает вычисленные SendAfter по Id — нужны отложенным подписчикам ключа.
    /// </summary>
    internal static async Task<Dictionary<long, DateTime>> MarkFailedAsync(
        DbContext db,
        IReadOnlyCollection<(long Id, string Error)> failed,
        IReadOnlyDictionary<long, int> attempts,
        CancellationToken ct,
        TimeProvider? time = null)
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
        var clock = time ?? TimeProvider.System;
        var sendAfters = new Dictionary<long, DateTime>(failed.Count);
        foreach (var (id, error) in failed)
        {
            attempts.TryGetValue(id, out var attempt);
            // Было 2^Attempt без cap — Attempt=20 давал ~12 суток и переполнение.
            var delaySeconds = Math.Min(Math.Pow(2, Math.Min(attempt, 10)) * (0.8 + Random.Shared.NextDouble() * 0.4), 3600);
            var sendAfter = clock.GetUtcNow().UtcDateTime.AddSeconds(delaySeconds);
            sendAfters[id] = sendAfter;
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
        return sendAfters;
    }
}
