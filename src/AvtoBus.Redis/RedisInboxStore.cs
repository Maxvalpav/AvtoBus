using AvtoBus.Runtime;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace AvtoBus.Redis;

/// <summary>
/// Redis-стор inbox-дедупликации (аудит 04 §1.2): <c>SET key "" NX EX window</c> —
/// атомарный claim без Lua. Один стор на все инстансы: мульти-инстанс без PostgreSQL.
/// Ключ: <c>avtobus:inbox:{consumer}:{messageId:N}</c> (consumer — имя очереди/топика,
/// MessageId без дефисов — короткий ключ).
///
/// Отказ Redis — fail-open (обработать + warning в лог): дедуп здесь best-effort,
/// at-least-once сохраняется, а хендлеры обязаны быть идемпотентными. Fail-closed
/// превратил бы падение кэша в остановку всей обработки.
/// </summary>
public sealed class RedisInboxStore : IInboxStore, IDisposable
{
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly TimeSpan _window;
    private readonly ILogger<RedisInboxStore>? _log;
    private readonly bool _ownsConnection;
    private int _disposed;

    public RedisInboxStore(RedisOptions options, TimeSpan window, ILogger<RedisInboxStore>? log = null)
        : this(Open(options, window), window, log, ownsConnection: true)
    {
    }

    /// <summary>Валидация окна — до коннекта, чтобы плохой конфиг падал сразу, а не после сетевого ожидания.</summary>
    private static ConnectionMultiplexer Open(RedisOptions options, TimeSpan window)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        return ConnectionMultiplexer.Connect(options.Configuration);
    }

    public RedisInboxStore(ConnectionMultiplexer redis, TimeSpan window, ILogger<RedisInboxStore>? log = null)
        : this(redis, window, log, ownsConnection: false)
    {
    }

    private RedisInboxStore(ConnectionMultiplexer redis, TimeSpan window, ILogger<RedisInboxStore>? log, bool ownsConnection)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        _redis = redis;
        _db = redis.GetDatabase();
        _window = window;
        _log = log;
        _ownsConnection = ownsConnection;
    }

    public async ValueTask<bool> TryMarkProcessedAsync(Guid messageId, string consumer, CancellationToken ct = default)
    {
        try
        {
            return await _db.StringSetAsync(Key(messageId, consumer), string.Empty, _window, When.NotExists).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            _log?.LogWarning(ex, "Redis inbox недоступен — дедуп пропущен (fail-open) для {MessageId} на {Consumer}", messageId, consumer);
            return true;
        }
    }

    public void Forget(Guid messageId, string consumer)
    {
        try
        {
            _db.KeyDelete(Key(messageId, consumer));
        }
        catch (RedisException ex)
        {
            _log?.LogWarning(ex, "Redis inbox недоступен — Forget пропущен для {MessageId} на {Consumer}", messageId, consumer);
        }
    }

    internal static string Key(Guid messageId, string consumer) => $"avtobus:inbox:{consumer}:{messageId:N}";

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        if (_ownsConnection)
            _redis.Dispose();
    }
}
