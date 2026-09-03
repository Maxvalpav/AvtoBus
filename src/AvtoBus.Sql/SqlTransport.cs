using System.Collections.Concurrent;
using AvtoBus.Observability;
using Npgsql;

namespace AvtoBus.Sql;

/// <summary>
/// SQL-транспорт (идеи 66–67): PostgreSQL таблица-очередь.
///
/// Семантика:
/// — Очередь — одна таблица: консьюмеры группы делят сообщения через FOR UPDATE SKIP LOCKED
///   (конкурентные читатели не блокируют друг друга, идея 66).
/// — Топик — базовая таблица сообщений + копия на каждую группу консьюмеров (fan-out как Kafka);
///   копирование идёт по high-water mark из мета-таблицы.
/// — Мгновенное пробуждение — LISTEN/NOTIFY после INSERT (идея 67): опрос с таймаутом ListenTimeout
///   вместо безудержного поллинга.
/// — Зависшие сообщения: claim истекает через ReclaimTimeout — сообщение возвращается в доставку.
/// — Reject(requeue) = сброс claim + visible_at=now + инкремент DeliveryAttempt; Reject(без requeue) = DELETE.
/// </summary>
public sealed class SqlTransport : ITransport, IConsumerLagProvider, IDisposable
{
    private readonly SqlOptions _options;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ConcurrentDictionary<string, long> _consumerLags = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _ensuredTopicTables = new(StringComparer.Ordinal);
    private long _lastLagCheckTicks;
    private int _disposed;

    public SqlTransport(SqlOptions options)
    {
        _options = options;
        var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
        _dataSource = builder.Build();
    }

    public string Name => "sql";

    /// <summary>Приближение лага: число доставленных, но не подтверждённых сообщений.</summary>
    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    // ── Send ──

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        var (table, channel) = TableNames(destination);
        var blob = SqlEnvelopeSerializer.ToBlob(envelope);
        var visibleAt = envelope.DeliverAt?.UtcDateTime ?? DateTime.UtcNow;

        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using (var command = new NpgsqlCommand(
                         $"INSERT INTO {table}(envelope, visible_at) VALUES(@envelope, @visible_at)", connection))
        {
            command.Parameters.AddWithValue("envelope", blob);
            command.Parameters.AddWithValue("visible_at", visibleAt);
            try
            {
                await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (NpgsqlException exception) when (IsMissingTable(exception))
            {
                // Таблица появилась после Provision (например, DLQ топика создаётся лениво) —
                // создаём на лету и повторяем вставку.
                await EnsureTableAsync(connection, table, ct).ConfigureAwait(false);
                await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }

        // Идея 67: мгновенное пробуждение консьюмеров, слушающих канал.
        await using (var notify = new NpgsqlCommand($"NOTIFY {channel}", connection))
            await notify.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>PostgreSQL-код «relation does not exist» (42P01).</summary>
    private static bool IsMissingTable(NpgsqlException exception)
        => exception.SqlState == "42P01";

    // ── Receive ──

    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var destination = subscription.Destination;
        var group = subscription.ConsumerGroup;
        var consumerName = $"{group}-{Guid.NewGuid():N}";

        var (sourceTable, channel) = TableNames(destination);
        var readTable = destination.Kind == DestinationKind.Topic
            ? TopicGroupTable(destination, group)
            : sourceTable;

        // LISTEN-канал для мгновенного пробуждения (идея 67).
        // Соединение пересоздаём при сбоях ожидания (см. WaitForNotificationAsync):
        // держать одно вечно нельзя — отменённое ожидание может отравить его состояние.
        var listenConnection = await OpenAsync(ct).ConfigureAwait(false);
        try
        {
            await using (var listen = new NpgsqlCommand($"LISTEN {channel}", listenConnection))
                await listen.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            while (!ct.IsCancellationRequested)
            {
                List<(long Id, Envelope Envelope)> claimed;
                try
                {
                    if (destination.Kind == DestinationKind.Topic)
                        await CopyNewTopicMessagesAsync(sourceTable, readTable, group, ct).ConfigureAwait(false);

                    claimed = await ClaimBatchAsync(readTable, consumerName, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    yield break;
                }
                catch (NpgsqlException)
                {
                    // Разрыв соединения — переподключаемся следующим циклом.
                    await Task.Delay(200, ct).ConfigureAwait(false);
                    continue;
                }

                if (claimed.Count == 0)
                {
                    listenConnection = await WaitForNotificationAsync(listenConnection, channel, ct).ConfigureAwait(false);
                    continue;
                }

                foreach (var (id, envelope) in claimed)
                {
                    TrackLag(readTable, ct);
                    yield return new SqlMessage(this, readTable, id, envelope, LogicalName(readTable));
                }
            }
        }
        finally
        {
            try { await listenConnection.DisposeAsync().ConfigureAwait(false); } catch { }
        }
    }

    /// <summary>
    /// Ждёт NOTIFY до <see cref="SqlOptions.ListenTimeout"/> и возвращает (возможно новое)
    /// LISTEN-соединение. Предыдущая реализация оборачивала <c>WaitAsync</c> в
    /// <c>Task.WaitAsync(timeout)</c>: внутреннее ожидание продолжало висеть на соединении,
    /// и следующий <c>WaitAsync</c> падал с «already in state 'Waiting'» — раннер умирал
    /// на втором пустом полле. Здесь ожидание отменяется токеном, а отравленное
    /// соединение пересоздаётся вместе с подпиской LISTEN.
    /// </summary>
    private async ValueTask<NpgsqlConnection> WaitForNotificationAsync(
        NpgsqlConnection listenConnection, string channel, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeoutCts.CancelAfter(_options.ListenTimeout);
                await listenConnection.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
                return listenConnection;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // ListenTimeout истёк — обычный полл-цикл, не ошибка.
                return listenConnection;
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                // Ожидание отравило соединение (или разрыв) — пересоздаём с новым LISTEN.
                try { await listenConnection.DisposeAsync().ConfigureAwait(false); } catch { }
                try
                {
                    listenConnection = await OpenAsync(ct).ConfigureAwait(false);
                    await using var listen = new NpgsqlCommand($"LISTEN {channel}", listenConnection);
                    await listen.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    await Task.Delay(200, ct).ConfigureAwait(false);
                }
            }
        }
    }

    private async Task<List<(long Id, Envelope Envelope)>> ClaimBatchAsync(string table, string consumerName, CancellationToken ct)
    {
        var result = new List<(long Id, Envelope Envelope)>();

        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        // Идея 66: FOR UPDATE SKIP LOCKED — конкурирующие читатели не ждут друг друга.
        // Зависшие (claim старше ReclaimTimeout) возвращаются в доставку.
        var reclaim = DateTime.UtcNow - _options.ReclaimTimeout;
        await using var select = new NpgsqlCommand(
            $"""
             SELECT id, envelope FROM {table}
             WHERE visible_at <= NOW()
               AND (claimed_at IS NULL OR claimed_at < @reclaim)
             ORDER BY id
             FOR UPDATE SKIP LOCKED
             LIMIT {_options.BatchSize}
             """, connection, transaction);
        select.Parameters.AddWithValue("reclaim", reclaim);

        await using (var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var id = reader.GetInt64(0);
                var blob = (byte[])reader.GetValue(1);
                try
                {
                    result.Add((id, SqlEnvelopeSerializer.FromBlob(blob)));
                }
                catch (InvalidDataException)
                {
                    // Мусор в очереди — удаляем, чтобы не зациклиться на нём.
                    result.Add((id, GarbageEnvelope()));
                }
            }
        }

        if (result.Count > 0)
        {
            // Помечаем как доставленные, чтобы другой консьюмер не подхватил.
            var ids = result.Select(r => r.Id).ToArray();
            await using var claim = new NpgsqlCommand(
                $"UPDATE {table} SET claimed_at = NOW(), claimed_by = @consumer WHERE id = ANY(@ids) AND claimed_at IS NULL",
                connection, transaction);
            claim.Parameters.AddWithValue("consumer", consumerName);
            claim.Parameters.AddWithValue("ids", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint, ids);
            await claim.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>
    /// Топик: копируем новые сообщения базовой таблицы в таблицу группы (fan-out).
    /// High-water mark — в мета-таблице; строка блокируется FOR UPDATE, чтобы один батч
    /// не скопировался дважды двумя консьюмерами одной группы.
    /// </summary>
    private async Task CopyNewTopicMessagesAsync(string sourceTable, string groupTable, string group, CancellationToken ct)
    {
        var metaTable = _options.TablePrefix + "topic_meta";

        var ensuredKey = $"{groupTable}|{metaTable}";
        var needEnsure = !_ensuredTopicTables.ContainsKey(ensuredKey);

        if (needEnsure)
        {
            await using var ensureConn = await OpenAsync(ct).ConfigureAwait(false);
            await using (var ensureGroup = new NpgsqlCommand(
                              $"CREATE TABLE IF NOT EXISTS {groupTable} (id BIGSERIAL PRIMARY KEY, envelope BYTEA NOT NULL, visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), claimed_at TIMESTAMPTZ, claimed_by TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
                              ensureConn))
                await ensureGroup.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            await using (var ensureMeta = new NpgsqlCommand(
                              $"CREATE TABLE IF NOT EXISTS {metaTable} (topic TEXT NOT NULL, grp TEXT NOT NULL, last_id BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (topic, grp))",
                              ensureConn))
                await ensureMeta.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            _ensuredTopicTables.TryAdd(ensuredKey, 1);
        }

        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        // Гарантируем строку меты и блокируем её для группы.
        await using (var insertMeta = new NpgsqlCommand(
                         $"INSERT INTO {metaTable}(topic, grp) VALUES(@topic, @grp) ON CONFLICT DO NOTHING",
                         connection, transaction))
        {
            insertMeta.Parameters.AddWithValue("topic", sourceTable);
            insertMeta.Parameters.AddWithValue("grp", group);
            await insertMeta.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var lockMeta = new NpgsqlCommand(
                         $"SELECT last_id FROM {metaTable} WHERE topic = @topic AND grp = @grp FOR UPDATE",
                         connection, transaction))
        {
            lockMeta.Parameters.AddWithValue("topic", sourceTable);
            lockMeta.Parameters.AddWithValue("grp", group);

            long lastId;
            await using (var reader = await lockMeta.ExecuteReaderAsync(ct).ConfigureAwait(false))
            {
                await reader.ReadAsync(ct).ConfigureAwait(false);
                lastId = reader.GetInt64(0);
            }

            await using (var copy = new NpgsqlCommand(
                             $"""
                              INSERT INTO {groupTable}(envelope, visible_at)
                              SELECT envelope, visible_at FROM {sourceTable}
                              WHERE id > @last_id
                              ORDER BY id
                              """, connection, transaction))
            {
                copy.Parameters.AddWithValue("last_id", lastId);
                await copy.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await using (var advance = new NpgsqlCommand(
                             $"UPDATE {metaTable} SET last_id = COALESCE((SELECT MAX(id) FROM {sourceTable}), last_id) WHERE topic = @topic AND grp = @grp",
                             connection, transaction))
            {
                advance.Parameters.AddWithValue("topic", sourceTable);
                advance.Parameters.AddWithValue("grp", group);
                await advance.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
    }

    private void TrackLag(string table, CancellationToken ct)
    {
        var nowTicks = DateTimeOffset.UtcNow.UtcTicks;
        var lastTicks = Interlocked.Read(ref _lastLagCheckTicks);
        if (lastTicks != 0 && new DateTimeOffset(lastTicks, TimeSpan.Zero) + TimeSpan.FromSeconds(5) > DateTimeOffset.UtcNow)
            return;
        Interlocked.Exchange(ref _lastLagCheckTicks, nowTicks);

        _ = Task.Run(async () =>
        {
            try
            {
                var reclaim = DateTime.UtcNow - _options.ReclaimTimeout;
                await using var connection = await OpenAsync(CancellationToken.None).ConfigureAwait(false);
                await using var count = new NpgsqlCommand(
                    $"SELECT COUNT(*) FROM {table} WHERE visible_at <= NOW() AND (claimed_at IS NULL OR claimed_at < @reclaim)",
                    connection);
                count.Parameters.AddWithValue("reclaim", reclaim);
                var value = await count.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
                _consumerLags[table] = Convert.ToInt64(value);
            }
            catch
            {
                // Метрика — наблюдательная.
            }
        }, CancellationToken.None);
    }

    // ── Topology ──

    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);

        foreach (var destination in destinations)
        {
            var (table, _) = TableNames(destination);
            await EnsureTableAsync(connection, table, ct).ConfigureAwait(false);

            // DLQ-таблицы создаём заранее (идея 164): отправка в error/poison не должна
            // падать на отсутствующей таблице, как in-memory очереди при Provision.
            await EnsureTableAsync(connection, TableNames(TransportDestination.Queue($"{destination.Name}.error")).Table, ct).ConfigureAwait(false);
            await EnsureTableAsync(connection, TableNames(TransportDestination.Queue($"{destination.Name}.poison")).Table, ct).ConfigureAwait(false);

            if (destination.Kind == DestinationKind.Topic)
            {
                // Базовая таблица топика создаётся как обычная; таблицы групп — лениво,
                // мета — при первом копировании. Здесь только базовая.
            }
        }
    }

    private static async Task EnsureTableAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {table} (
                 id BIGSERIAL PRIMARY KEY,
                 envelope BYTEA NOT NULL,
                 visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 claimed_at TIMESTAMPTZ,
                 claimed_by TEXT,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             );
             CREATE INDEX IF NOT EXISTS ix_{Sanitize(table)}_visible
                 ON {table}(visible_at) WHERE claimed_at IS NULL;
             """, connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    // ── Helpers ──

    private async ValueTask<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        return connection;
    }

    private (string Table, string Channel) TableNames(TransportDestination destination)
    {
        var name = Sanitize(destination.Name);
        return ($"{_options.TablePrefix}{name}", $"avtobus_{name}");
    }

    /// <summary>
    /// Имя назначения из физической таблицы: снимает префикс. Нужно для <see cref="ITransportMessage.Source"/>,
    /// чтобы повторная отправка (delayed retry, DLQ) шла в правильную таблицу, а не получала префикс дважды.
    /// </summary>
    private string LogicalName(string table)
        => table.StartsWith(_options.TablePrefix, StringComparison.Ordinal)
            ? table[_options.TablePrefix.Length..]
            : table;

    private string TopicGroupTable(TransportDestination destination, string group)
        => $"{_options.TablePrefix}topic_{Sanitize(destination.Name)}_{Sanitize(group)}";

    private static string Sanitize(string name)
    {
        var builder = new System.Text.StringBuilder(name.Length);
        foreach (var ch in name)
            builder.Append(char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_');
        return builder.ToString();
    }

    private static Envelope GarbageEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "avtobus.garbage",
        Body = Array.Empty<byte>(),
    };

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        _dataSource.Dispose();
    }

    /// <summary>ITransport : IAsyncDisposable — соединения короткоживущие, синхронного Dispose достаточно.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Сообщение из SQL-очереди. Ack = DELETE; Reject(requeue) = сброс claim + visible_at=now
    /// + инкремент DeliveryAttempt (пере-сериализация конверта); Reject(без requeue) = DELETE
    /// (вне доставки; DLQ — на уровне ядра).
    /// </summary>
    private sealed class SqlMessage : ITransportMessage
    {
        private readonly SqlTransport _transport;
        private readonly string _table;
        private readonly long _id;
        private readonly string _sourceName;
        private int _settled;

        public SqlMessage(SqlTransport transport, string table, long id, Envelope envelope, string sourceName)
        {
            _transport = transport;
            _table = table;
            _id = id;
            _sourceName = sourceName;
            Envelope = envelope;
        }

        public Envelope Envelope { get; }

        /// <summary>Логическое имя очереди: повторная отправка в неё не дублирует префикс.</summary>
        public TransportDestination Source => TransportDestination.Queue(_sourceName);

        public async ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            await _transport.ExecuteAsync($"DELETE FROM {_table} WHERE id = @id", _id, ct).ConfigureAwait(false);
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue)
            {
                // Requeue: снимаем claim и делаем видимым снова; попытка инкрементируется.
                var blob = SqlEnvelopeSerializer.ToBlob(Envelope.NextAttempt());
                await _transport.ExecuteRequeueAsync(_table, _id, blob, ct).ConfigureAwait(false);
            }
            else
            {
                await _transport.ExecuteAsync($"DELETE FROM {_table} WHERE id = @id", _id, ct).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask ExecuteAsync(string sql, long id, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("id", id);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async ValueTask ExecuteRequeueAsync(string table, long id, byte[] blob, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"UPDATE {table} SET visible_at = NOW(), claimed_at = NULL, claimed_by = NULL, envelope = @envelope WHERE id = @id",
            connection);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("envelope", blob);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
