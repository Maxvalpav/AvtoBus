using System.Runtime.CompilerServices;
using AvtoBus.Observability;
using Microsoft.Extensions.Logging;
using Xunit;

namespace AvtoBus.Tests;

public class RateLimitedLoggerTests
{
    private sealed class CollectingLogger : ILogger<object>
    {
        public readonly List<(LogLevel Level, string Message)> Records = [];
        public bool Enabled = true;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => Enabled;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Records.Add((logLevel, formatter(state, exception)));
    }

    [Fact]
    public void Same_error_is_logged_only_at_1_10_100_occurrences()
    {
        var inner = new CollectingLogger();
        var logger = new RateLimitedLogger<object>(inner);
        var error = new InvalidOperationException("boom");

        for (var i = 1; i <= 1000; i++)
            logger.Log(LogLevel.Error, 0, "state", error, (_, _) => "same error");

        // Отлогировано: 1, 10, 100, 1000 → 4 записи, а не 1000.
        Assert.Equal(4, inner.Records.Count);
        Assert.All(inner.Records, r => Assert.Equal(LogLevel.Error, r.Level));
    }

    [Fact]
    public void Distinct_messages_are_not_suppressed()
    {
        var inner = new CollectingLogger();
        var logger = new RateLimitedLogger<object>(inner);

        for (var i = 0; i < 50; i++)
            logger.Log(LogLevel.Error, 0, "state", null, (_, _) => $"unique-{i}");

        Assert.Equal(50, inner.Records.Count);
    }

    [Fact]
    public void Information_level_passes_through_always()
    {
        var inner = new CollectingLogger();
        var logger = new RateLimitedLogger<object>(inner);

        for (var i = 0; i < 100; i++)
            logger.Log(LogLevel.Information, 0, "state", null, (_, _) => "info");

        Assert.Equal(100, inner.Records.Count);
    }
}
