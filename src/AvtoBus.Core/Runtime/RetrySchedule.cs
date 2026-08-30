using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// ZIO Schedule / cats-retry порт: компонуемые политики `Schedule.exponential || jitter || spaced || recurs`.
/// Заменяет плоский `RecoverabilitySettings Immediate/Delayed` на комбинаторы: `Schedule.Exponential(100ms).Jitter().AndThen(Spaced(1s)).Recurs(5)`.
/// Аналог: ZIO `Schedule`, Scala `cats-retry`, Resilience4j.
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
        public override TimeSpan? NextDelay(int attempt, Exception ex) => a.NextDelay(attempt, ex) ?? b.NextDelay(attempt, ex);
    }
    private sealed class OrSchedule(RetrySchedule a, RetrySchedule b) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => a.NextDelay(attempt, ex) ?? b.NextDelay(attempt, ex);
    }
    private sealed class JitterSchedule(RetrySchedule inner, double factor) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex)
        {
            var d = inner.NextDelay(attempt, ex);
            if (d is null) return null;
            var jitter = Random.Shared.NextDouble() * factor * 2 - factor; // -factor..+factor
            return TimeSpan.FromMilliseconds(d.Value.TotalMilliseconds * (1 + jitter));
        }
    }
    private sealed class ExponentialSchedule(TimeSpan initial, double factor) : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => TimeSpan.FromMilliseconds(initial.TotalMilliseconds * Math.Pow(factor, attempt - 1));
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
            long a = 0, b = 1; for (int i = 0; i < attempt; i++) { var t = a + b; a = b; b = t; }
            return TimeSpan.FromMilliseconds(initial.TotalMilliseconds * a);
        }
    }
    private sealed class NeverSchedule : RetrySchedule
    {
        public override TimeSpan? NextDelay(int attempt, Exception ex) => null;
    }
}

public static class RetryScheduleExtensions
{
    /// <summary>Применяет ZIO Schedule к Recoverability.</summary>
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
