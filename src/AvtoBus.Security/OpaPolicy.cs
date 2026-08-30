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
    private static readonly System.Text.RegularExpressions.Regex TenantRegex =
        new(@"input\.tenant\s*==\s*""([^""]+)""", System.Text.RegularExpressions.RegexOptions.Compiled, TimeSpan.FromMilliseconds(100));

    private static readonly System.Text.RegularExpressions.Regex RoleRegex =
        new(@"principal\.role\s*==\s*""([^""]+)""", System.Text.RegularExpressions.RegexOptions.Compiled, TimeSpan.FromMilliseconds(100));

    public bool IsAllowed(ConsumeContext ctx, string policy)
    {
        if (string.IsNullOrWhiteSpace(policy) || policy.Contains("allow { true }", StringComparison.Ordinal)) return true;
        if (policy.Contains("deny", StringComparison.Ordinal))
            return false;

        // Стаб: реальный путь — `opa eval` через `OPA WASM` или `Rego.NET`.
        if (policy.Contains("input.tenant", StringComparison.Ordinal))
        {
            var m = TenantRegex.Match(policy);
            if (m.Success) return ctx.Envelope.TenantId == m.Groups[1].Value;
        }
        if (policy.Contains("principal.role", StringComparison.Ordinal))
        {
            var m = RoleRegex.Match(policy);
            if (m.Success) return ctx.Principal?.IsInRole(m.Groups[1].Value) == true;
        }
        // Fail closed by default — unknown policy denies
        return false;
    }
}

public sealed class OpaAuthorizationMiddleware(IOpaEvaluator eval, OpaOptions opts) : AvtoBus.Pipeline.IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, AvtoBus.Pipeline.BusDelegate next)
    {
        var allowed = eval.IsAllowed(context, opts.Policy);
        if (!allowed && opts.FailClosed)
        {
            context.DeadLetter($"OPA deny: {opts.Policy}");
            return ValueTask.CompletedTask;
        }
        if (!allowed)
            return ValueTask.CompletedTask;

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
