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

    private static readonly TimeSpan WindowLength = TimeSpan.FromMinutes(5);

    private readonly ILogger<T> _inner;

    public RateLimitedLogger(ILoggerFactory factory) => _inner = factory.CreateLogger<T>();

    public RateLimitedLogger(ILogger<T> inner) => _inner = inner;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        => _inner.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        // Семплируются только ошибки и предупреждения: шторм информационных логов редок,
        // а подавление полезных debug-записей мешает диагностике.
        if (logLevel < LogLevel.Warning)
        {
            _inner.Log(logLevel, eventId, state, exception, formatter);
            return;
        }

        var key = exception?.GetType().FullName + " | " + formatter(state, exception);
        var count = Samples.GetOrAdd(key, static _ => new Window()).Increment();

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
        private readonly object Gate = new();
        private DateTimeOffset Starts = TimeProvider.System.GetUtcNow();
        private long Seen;

        public long Increment()
        {
            lock (Gate)
            {
                var now = TimeProvider.System.GetUtcNow();
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
