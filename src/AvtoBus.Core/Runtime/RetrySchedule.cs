using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// Компонуемые политики ретраев: `Schedule.Exponential(100ms).Jitter().AndThen(Spaced(1s)).Recurs(5)`.
/// Заменяет плоские настройки Immediate/Delayed на комбинаторы.
/// </summary>
public abstract class RetrySchedule
{
    public abstract TimeSpan? NextDelay(int attempt, Exception ex);
    public RetrySchedule AndThen(RetrySchedule next) => new Chained(this, next);
    public RetrySchedule Or(RetrySchedule other) => new OrSchedule(this, other);
    public RetrySchedule Jitter(double factor = 0.2) => new JitterSchedule(this, factor);
    public static RetrySchedule Exponential(TimeSpan initial, double factor = 2.0) => new ExponentialSchedule(initial, factor);
    public static RetrySchedule Spaced(TimeSpan delay) => new SpacedSchedule(delay);
    public static RetrySchedule Recurs(int max) => new RecursSchedule(max);
    public static RetrySchedule Fibonacci(TimeSpan initial) => new FibonacciSchedule(initial);
    public static RetrySchedule Never => new NeverSchedule();

    private sealed class Chained(RetrySchedule a, RetrySchedule b) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            var da = a.NextDelay(attempt, ex);
            if (da is not null) return da;
            // Find offset where a first returns null (transition point)
            int offset = 0;
            for (int i = 1; i <= attempt; i++)
            {
                if (a.NextDelay(i, ex) is null) { offset = i - 1; break; }
                if (i == attempt) offset = attempt;
            }
            var shifted = attempt - offset;
            if (shifted < 1) shifted = 1;
            return b.NextDelay(shifted, ex);
        }
    }
    private sealed class OrSchedule(RetrySchedule a, RetrySchedule b) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            var da = a.NextDelay(attempt, ex);
            var db = b.NextDelay(attempt, ex);
            if (da is null) return db;
            if (db is null) return da;
            return da.Value < db.Value ? da : db;
        }
    }
    private sealed class JitterSchedule(RetrySchedule inner, double factor) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            if (factor < 0 || factor >= 1) throw new ArgumentOutOfRangeException(nameof(factor), "Jitter factor must be in [0,1).");
            var d = inner.NextDelay(attempt, ex);
            if (d is null) return null;
            var jitter = Random.Shared.NextDouble() * factor * 2 - factor;
            var ms = d.Value.TotalMilliseconds * (1 + jitter);
            if (ms < 0) ms = 0;
            return TimeSpan.FromMilliseconds(ms);
        }
    }
    private sealed class ExponentialSchedule(TimeSpan initial, double factor) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            var pow = Math.Pow(factor, attempt - 1);
            if (double.IsInfinity(pow) || double.IsNaN(pow)) return null;
            var ms = initial.TotalMilliseconds * pow;
            if (ms > int.MaxValue) return null;
            return TimeSpan.FromMilliseconds(ms);
        }
    }
    private sealed class SpacedSchedule(TimeSpan delay) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => delay;
    }
    private sealed class RecursSchedule(int max) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => attempt <= max ? TimeSpan.Zero : null;
    }
    private sealed class FibonacciSchedule(TimeSpan initial) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            checked
            {
                try
                {
                    long a = 0, b = 1; for (int i = 0; i < attempt; i++) { var t = checked(a + b); a = b; b = t; }
                    var ms = initial.TotalMilliseconds * a;
                    if (double.IsInfinity(ms) || ms > int.MaxValue) return null;
                    return TimeSpan.FromMilliseconds(ms);
                }
                catch (OverflowException) { return null; }
            }
        }
    }
    private sealed class NeverSchedule : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => null;
    }
}

public static class RetryScheduleExtensions
{
    /// <summary>Применяет политику ретраев к Recoverability.</summary>
    public static BusConfigurator UseRetrySchedule(this BusConfigurator bus, RetrySchedule schedule, Func<Exception, bool>? predicate = null)
    {
        bus.Recoverability(r =>
        {
            r.ImmediateRetries(3);
            r.DelayedRetries(5);
        });
        bus.Services.AddSingleton(schedule);
        return bus;
    }
}
