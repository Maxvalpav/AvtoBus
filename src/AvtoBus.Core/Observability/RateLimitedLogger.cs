using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Observability;

/// <summary>
/// Защита от лог-шторма (идея 335): одинаковое сообщение ошибки в окне фиксируется
/// только на 1-й, 10-й, 100-й (и далее степени 10) раз, остальные вхождения подавляются.
/// Подключение — одним декоратором:
/// <code>
/// services.AddSingleton(typeof(ILogger&lt;&gt;), typeof(RateLimitedLogger&lt;&gt;));
/// </code>
/// </summary>
public sealed class RateLimitedLogger<T> : ILogger<T>
{
    private static readonly ConcurrentDictionary<string, Window> Samples = new(StringComparer.Ordinal);
    private const int MaxKeys = 10_000;

    private static readonly TimeSpan WindowLength = TimeSpan.FromMinutes(5);

    private readonly ILogger<T> _inner;
    private readonly TimeProvider _time;

    public RateLimitedLogger(ILoggerFactory factory) : this(factory.CreateLogger<T>(), TimeProvider.System) { }
    public RateLimitedLogger(ILoggerFactory factory, TimeProvider time) => (_inner, _time) = (factory.CreateLogger<T>(), time);

    public RateLimitedLogger(ILogger<T> inner) => (_inner, _time) = (inner, TimeProvider.System);
    public RateLimitedLogger(ILogger<T> inner, TimeProvider time) => (_inner, _time) = (inner, time);

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        => _inner.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => _inner.IsEnabled(logLevel);

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (logLevel < LogLevel.Warning)
        {
            _inner.Log(logLevel, eventId, state, exception, formatter);
            return;
        }

        if (!_inner.IsEnabled(logLevel)) return;

        var formatted = formatter(state, exception);
        // Avoid cardinality explosion: truncate and hash long messages (MessageId etc)
        if (formatted.Length > 200) formatted = formatted[..200];
        var key = (exception?.GetType().FullName ?? "no-ex") + " | " + formatted;
        if (Samples.Count >= MaxKeys && !Samples.ContainsKey(key)) return; // shed new keys when full
        var win = Samples.GetOrAdd(key, _ => new Window(_time));
        var count = win.Increment();

        if (IsReportingMoment(count))
            _inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>1, 10, 100, 1000… — первые вхождения пишутся, середина подавлена.</summary>
    private static bool IsReportingMoment(long count)
    {
        while (count >= 10)
        {
            if (count % 10 != 0)
                return false;
            count /= 10;
        }

        return count == 1;
    }

    private sealed class Window
    {
        private readonly TimeProvider _time;
        private readonly object Gate = new();
        private DateTimeOffset Starts;
        private long Seen;
        public Window(TimeProvider time) { _time = time; Starts = time.GetUtcNow(); }

        public long Increment()
        {
            lock (Gate)
            {
                var now = _time.GetUtcNow();
                if (now - Starts > WindowLength)
                {
                    Starts = now;
                    Seen = 0;
                }

                return ++Seen;
            }
        }
    }
}
