using Microsoft.Extensions.Diagnostics.HealthChecks;
using AvtoBus.Runtime;

namespace AvtoBus.Observability;

/// <summary>
/// HealthCheck для N-1 деплоя (идея 35): Unhealthy пока дрейнится, Degraded если цепь разомкнута или лаг выше порога.
/// Регистрируется через <c>services.AddAvtoBusHealthCheck()</c> и <c>app.MapHealthChecks("/health")</c>.
/// </summary>
public sealed class AvtoBusHealthCheck : IHealthCheck
{
    private readonly ConsumerHost _host;
    private readonly long _lagThreshold;

    public AvtoBusHealthCheck(ConsumerHost host, long lagThreshold = 10_000)
    {
        _host = host;
        _lagThreshold = lagThreshold;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var data = new Dictionary<string, object>
        {
            ["consumers"] = _host.Runners.Count,
            ["allReceivingStopped"] = _host.AllReceivingStopped,
        };

        foreach (var r in _host.Runners)
        {
            data[$"lag:{r.Name}"] = r.Lag;
            data[$"circuit:{r.Name}"] = r.CircuitState.ToString();
            data[$"processed:{r.Name}"] = r.Processed;
        }

        if (_host.AllReceivingStopped)
            return Task.FromResult(HealthCheckResult.Unhealthy("AvtoBus дрейнится — приём остановлен", data: data));

        var open = _host.Runners.Any(r => r.CircuitState == CircuitState.Open);
        if (open)
            return Task.FromResult(HealthCheckResult.Degraded("Circuit breaker разомкнут", data: data));

        var highLag = _host.ConsumerLags.Values.Any(v => v > _lagThreshold);
        if (highLag)
            return Task.FromResult(HealthCheckResult.Degraded($"Lag > {_lagThreshold}", data: data));

        return Task.FromResult(HealthCheckResult.Healthy("AvtoBus OK", data: data));
    }
}
