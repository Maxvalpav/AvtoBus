using AvtoBus.Pipeline;

namespace AvtoBus.Multitenancy;

/// <summary>
/// Per-tenant квота входящего трафика (идея 464): жирный тенант не должен вытеснять мелких.
/// Сообщение, превысившее квоту тенанта, откладывается (backpressure) — брокер вернёт его позже,
/// остальные тенанты продолжают обрабатываться без задержек.
/// </summary>
public sealed class TenantRateLimitMiddleware(TenantRegistry registry, TimeProvider clock) : IBusMiddleware
{
    private const int MaxLimiters = 2048;
    private readonly Dictionary<string, TenantRateLimiter> _limiters = new(StringComparer.Ordinal);

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var tenantId = context.Envelope.TenantId;
        if (tenantId is null)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // Whitelist mode: unknown tenant bypasses or deadletters
        if (!registry.Contains(tenantId))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        var rate = registry.InboundRateOf(tenantId);
        if (rate <= 0)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        var limiter = GetOrCreate(tenantId, rate);
        if (limiter.TryAcquire(clock.GetUtcNow()))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // Defer with bounded retry count using DeliveryAttempt (requeue increments it)
        if (context.Envelope.DeliveryAttempt >= 10)
        {
            context.DeadLetter($"Tenant {tenantId} over quota after {context.Envelope.DeliveryAttempt} deferrals");
            return;
        }

        await context.DeferAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
    }

    private TenantRateLimiter GetOrCreate(string tenantId, int rate)
    {
        lock (_limiters)
        {
            if (!_limiters.TryGetValue(tenantId, out var limiter))
            {
                if (_limiters.Count >= MaxLimiters)
                    return new TenantRateLimiter(rate); // transient limiter, no growth

                limiter = new TenantRateLimiter(rate);
                _limiters[tenantId] = limiter;
            }

            return limiter;
        }
    }
}

/// <summary>Фиксированное окно: не более N запросов в секунду на тенанта. Потокобезопасен.</summary>
internal sealed class TenantRateLimiter(int permitsPerSecond)
{
    private readonly object _sync = new();
    private int _count;
    private DateTimeOffset _windowStart;

    public bool TryAcquire(DateTimeOffset now)
    {
        lock (_sync)
        {
            if (_windowStart == default || now - _windowStart >= TimeSpan.FromSeconds(1))
            {
                _windowStart = now;
                _count = 0;
            }

            if (_count >= permitsPerSecond)
                return false;

            _count++;
            return true;
        }
    }
}
