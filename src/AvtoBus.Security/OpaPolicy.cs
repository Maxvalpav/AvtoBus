using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Security;

/// <summary>
/// ABAC-политика как код `allow { input.tenant == "eu" }` per-message.
/// Заменяет плоский `[BusAuthorize(Roles="admin")]` на выразительные правила.
/// Оценивается в `AuthorizationMiddleware` перед хендлером.
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
        // Пустоту решает middleware через OpaOptions.FailClosed; здесь пустая политика —
        // deny, чтобы прямой вызов эвалуатора тоже был fail-closed.
        if (string.IsNullOrWhiteSpace(policy)) return false;
        var trimmed = policy.Trim();
        if (trimmed == "allow { true }" || trimmed.Contains("allow { true }", StringComparison.Ordinal) && trimmed.Length < 30)
            return true;

        // Check explicit tenant/role rules before generic deny
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
        if (policy.TrimStart().StartsWith("deny", StringComparison.OrdinalIgnoreCase))
            return false;
        // Fail closed by default — unknown policy denies
        return false;
    }
}

public sealed class OpaAuthorizationMiddleware(IOpaEvaluator eval, OpaOptions opts, Microsoft.Extensions.Logging.ILogger<OpaAuthorizationMiddleware>? logger = null) : AvtoBus.Pipeline.IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, AvtoBus.Pipeline.BusDelegate next)
    {
        // Пустая политика — это невысказанное намерение: в fail-closed режиме запрещаем,
        // в audit-режиме пропускаем с пометкой.
        if (string.IsNullOrWhiteSpace(opts.Policy))
        {
            logger?.LogWarning("Policy: пустая политика для {MessageType} — {Decision}", context.Envelope.MessageType, opts.FailClosed ? "deny" : "audit-allow");
            if (opts.FailClosed)
            {
                context.DeadLetter("Policy deny: empty policy");
                return ValueTask.CompletedTask;
            }
            return next(context);
        }

        var allowed = eval.IsAllowed(context, opts.Policy);
        if (!allowed)
        {
            logger?.LogWarning("Policy deny {Policy} for {MessageType} {MessageId}", opts.Policy, context.Envelope.MessageType, context.Envelope.MessageId);
            if (opts.FailClosed)
                context.DeadLetter($"Policy deny: {opts.Policy}");
            else
                context.DeadLetter($"Policy deny (audit): {opts.Policy}");
            return ValueTask.CompletedTask;
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
