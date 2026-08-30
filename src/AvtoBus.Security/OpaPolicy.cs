using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Security;

/// <summary>
/// OPA/Rego ABAC порт (Go): политика как код `allow { input.tenant == "eu" }` per-message.
/// Заменяет плоский `[BusAuthorize(Roles="admin")]` на выразительные правила: `deny if input.pii and clearance LT 3`.
/// Аналог: OPA sidecar, Cedar, casbin. Оценивается в `AuthorizationMiddleware` перед хендлером.
/// </summary>
public sealed class OpaOptions
{
    public string Policy { get; set; } = "allow { true }";
    public bool FailClosed { get; set; } = true;
}

public interface IOpaEvaluator
{
    bool IsAllowed(ConsumeContext ctx, string policy);
}

public sealed class RegoEvaluator : IOpaEvaluator
{
    public bool IsAllowed(ConsumeContext ctx, string policy)
    {
        if (string.IsNullOrWhiteSpace(policy) || policy.Contains("allow { true }")) return true;
        if (policy.Contains("deny")) return false;
        // Стаб: реальный путь — `opa eval` через `OPA WASM` или `Rego.NET`. Парсим `input.tenant == "eu"` по Headers
        if (policy.Contains("input.tenant"))
        {
            var m = System.Text.RegularExpressions.Regex.Match(policy, @"input\.tenant\s*==\s*""([^""]+)""");
            if (m.Success) return ctx.Envelope.TenantId == m.Groups[1].Value;
        }
        if (policy.Contains("principal.role"))
        {
            var m = System.Text.RegularExpressions.Regex.Match(policy, @"principal\.role\s*==\s*""([^""]+)""");
            if (m.Success) return ctx.Principal?.IsInRole(m.Groups[1].Value) == true;
        }
        return true;
    }
}

public sealed class OpaAuthorizationMiddleware(IOpaEvaluator eval, OpaOptions opts) : AvtoBus.Pipeline.IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, AvtoBus.Pipeline.BusDelegate next)
    {
        if (!eval.IsAllowed(context, opts.Policy))
        {
            if (opts.FailClosed) { context.DeadLetter($"OPA deny: {opts.Policy}"); return ValueTask.CompletedTask; }
        }
        return next(context);
    }
}

public static class OpaExtensions
{
    public static BusConfigurator UseOpa(this BusConfigurator bus, string regoPolicy, bool failClosed = true)
    {
        var opts = new OpaOptions { Policy = regoPolicy, FailClosed = failClosed };
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<IOpaEvaluator, RegoEvaluator>();
        bus.Services.AddSingleton<OpaAuthorizationMiddleware>();
        bus.Pipeline(b => b.Use<OpaAuthorizationMiddleware>());
        return bus;
    }
}
