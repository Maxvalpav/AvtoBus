using System.Collections.Concurrent;
using AvtoBus.Observability;
using Npgsql;

namespace AvtoBus.Sql;

/// <summary>
/// Сообщение из SQL-очереди. Ack = DELETE; Reject(requeue) = сброс claim;
/// Reject(без requeue) = DELETE. DLQ — на уровне ядра.
/// </summary>
internal sealed class SqlMessage : ITransportMessage
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
