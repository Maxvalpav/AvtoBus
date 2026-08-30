using System.Data;
using System.Data.Common;
using AvtoBus.Abstractions;
using Npgsql;

namespace AvtoBus.Persistence.Postgres;

public sealed class AvtoBusDbSession : IMessageDbSession, IAsyncDisposable
{
    private bool _completed;
    private readonly List<Action> _afterCommit = [];

    private AvtoBusDbSession(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction)
    {
        Connection = connection;
        Transaction = transaction;
    }

    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    DbConnection IMessageDbSession.Connection => Connection;
    DbTransaction IMessageDbSession.Transaction => Transaction;

    public void OnCommitted(Action callback) => _afterCommit.Add(callback);

    public static async ValueTask<AvtoBusDbSession> BeginAsync(
        NpgsqlDataSource dataSource,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        try
        {
            var transaction = await connection.BeginTransactionAsync(
                isolationLevel, cancellationToken);
            return new AvtoBusDbSession(connection, transaction);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        await Transaction.CommitAsync(cancellationToken);
        _completed = true;
        foreach (var callback in _afterCommit)
        {
            try { callback(); }
            catch { /* Commit уже состоялся; polling остается надежным fallback. */ }
        }
    }

    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        await Transaction.RollbackAsync(cancellationToken);
        _completed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try { await Transaction.RollbackAsync(); }
            catch { /* Preserve the original application exception. */ }
        }

        await Transaction.DisposeAsync();
        await Connection.DisposeAsync();
    }
}
