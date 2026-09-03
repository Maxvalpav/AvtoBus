using System.Security.Claims;
using System.Text;
using AvtoBus;
using AvtoBus.Security;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Регрессия дизайн-решений аудита безопасности:
/// подпись v2 покрывает маршрутизацию, avtobus-user fail-closed,
/// пустая OPA-политика запрещает, Tls fail-fast.
/// </summary>
public class SecurityDecisionsTests
{
    private static Envelope NewEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "test.thing",
        Body = Encoding.UTF8.GetBytes("{\"x\":1}"),
        ContentType = "application/json",
        SentAt = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
    };

    private static EnvelopeSecurity Security(Action<SecurityOptions> configure)
    {
        var options = new SecurityOptions();
        configure(options);
        return new EnvelopeSecurity(options);
    }

    [Fact]
    public void V2_signature_covers_routing_fields()
    {
        var security = Security(o => { o.MasterSecret = "s2"; o.RequireSignature = true; });
        var signed = security.ProtectOutbound(NewEnvelope() with { ReplyTo = "reply-q", PartitionKey = "pk", Priority = 7 }, "svc");

        Assert.Equal("2", signed.Header("avtobus-sig-version"));
        Assert.NotNull(security.OpenInbound(signed));

        // Подмена ReplyTo/PartitionKey/Priority ломает v2-подпись.
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(signed with { ReplyTo = "evil-q" }));
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(signed with { PartitionKey = "evil" }));
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(signed with { Priority = 1 }));
    }

    [Fact]
    public void Legacy_v1_envelope_still_verifies_and_v1_emission_supported()
    {
        var v1 = Security(o => { o.MasterSecret = "s2"; o.RequireSignature = true; o.SignatureVersion = 1; });
        var signed = v1.ProtectOutbound(NewEnvelope(), "svc");
        Assert.Null(signed.Header("avtobus-sig-version"));

        // Новая сторона принимает старые подписи (in-flight при rollout).
        var current = Security(o => { o.MasterSecret = "s2"; o.RequireSignature = true; });
        Assert.NotNull(current.OpenInbound(signed));
    }

    [Fact]
    public void Stripped_version_header_does_not_downgrade_to_valid()
    {
        var security = Security(o => { o.MasterSecret = "s2"; o.RequireSignature = true; });
        var signed = security.ProtectOutbound(NewEnvelope(), "svc");

        // Атакующий стёр заголовок версии: v1-проверка v2-подписи обязана упасть.
        // (WithHeaders мержит заголовки, поэтому для стирания — прямое `with`.)
        var strippedHeaders = signed.Headers
            .Where(kv => kv.Key != "avtobus-sig-version")
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
        var stripped = signed with { Headers = strippedHeaders };
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(stripped));
    }

    [Fact]
    public void Signed_extractor_trusts_only_signed_user_header()
    {
        var security = Security(o => { o.MasterSecret = "s2"; o.RequireSignature = true; });
        var extractor = new SignedPrincipalExtractor(security);

        var principal = new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, "alice"), new Claim(ClaimTypes.Role, "admin")], "test"));
        var wire = PrincipalSerializer.Serialize(principal)!;

        var signed = security.ProtectOutbound(NewEnvelope().WithHeader(BusHeaders.User, wire), "svc");
        var extracted = extractor.Extract(signed);
        Assert.NotNull(extracted);
        Assert.Equal("alice", extracted.Identity?.Name);
        Assert.True(extracted.IsInRole("admin"));

        // Тот же заголовок без подписи — аноним, а не alice.
        Assert.Null(extractor.Extract(NewEnvelope().WithHeader(BusHeaders.User, wire)));
        Assert.Null(extractor.Extract(NewEnvelope()));
    }

    [Fact]
    public void Tls_options_fail_fast_instead_of_silent_ignore()
    {
        var options = new SecurityOptions { Tls = new TlsOptions { RequireClientCertificate = true } };
        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains("mTLS", ex.Message);
    }

    [Fact]
    public void Empty_opa_policy_denies_fail_closed()
    {
        Assert.False(new RegoEvaluator().IsAllowed(
            OpaContext(), ""));
        Assert.False(new RegoEvaluator().IsAllowed(
            OpaContext(), "   "));
    }

    [Fact]
    public void Opa_allow_substring_does_not_bypass()
    {
        // Приоритет ||/&& раньше разрешал любую политику <30 символов с подстрокой.
        Assert.False(new RegoEvaluator().IsAllowed(
            OpaContext(), "deny; allow { true }"));
        Assert.True(new RegoEvaluator().IsAllowed(
            OpaContext(), "allow { true }"));
    }

    [Fact]
    public async Task Opa_audit_mode_passes_to_next_instead_of_dlq()
    {
        var deny = new DenyAllEvaluator();
        var audit = new OpaAuthorizationMiddleware(deny, new OpaOptions { Policy = "deny all", FailClosed = false });
        var nextCalled = false;
        var ctx = OpaContext();
        await audit.InvokeAsync(ctx, _ => { nextCalled = true; return ValueTask.CompletedTask; });

        Assert.True(nextCalled);
        Assert.NotEqual(ConsumeOutcome.DeadLettered, ctx.Outcome);
    }

    [Fact]
    public async Task Opa_enforce_mode_dead_letters_and_skips_next()
    {
        var deny = new DenyAllEvaluator();
        var enforce = new OpaAuthorizationMiddleware(deny, new OpaOptions { Policy = "deny all", FailClosed = true });
        var nextCalled = false;
        var ctx = OpaContext();
        await enforce.InvokeAsync(ctx, _ => { nextCalled = true; return ValueTask.CompletedTask; });

        Assert.False(nextCalled);
        Assert.Equal(ConsumeOutcome.DeadLettered, ctx.Outcome);
    }

    private sealed class DenyAllEvaluator : IOpaEvaluator
    {
        public bool IsAllowed(ConsumeContext ctx, string policy) => false;
    }

    [Fact]
    public void Key_rotation_without_history_keeps_verifying()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "rotation-secret";
            o.RequireSignature = true;
            o.KeyRotationInterval = TimeSpan.FromHours(1);
            o.KeepPreviousKeyGenerations = 0;
        });

        var t0 = new DateTimeOffset(2026, 1, 1, 0, 30, 0, TimeSpan.Zero);
        security.RotateKeysIfDue(t0 + TimeSpan.FromHours(1));
        var signed = security.ProtectOutbound(NewEnvelope(), "svc");
        // Без RefreshSnapshot stale-снапшот ломал всю проверку после первой ротации.
        Assert.NotNull(security.OpenInbound(signed));

        security.RotateKeysIfDue(t0 + TimeSpan.FromHours(2));
        var signed2 = security.ProtectOutbound(NewEnvelope(), "svc");
        Assert.NotNull(security.OpenInbound(signed2));
    }

    [Fact]
    public void Decrypted_envelope_keeps_valid_signature()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "enc-secret";
            o.RequireSignature = true;
            o.EncryptBody = true;
        });

        var principal = new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, "bob")], "test"));
        var outgoing = security.ProtectOutbound(
            NewEnvelope().WithHeader(BusHeaders.User, PrincipalSerializer.Serialize(principal)!), "svc");
        var opened = security.OpenInbound(outgoing);

        // Подпись была по шифртексту: после расшифровки перештамповываем,
        // иначе SignedPrincipalExtractor отклонял бы свои же сообщения.
        Assert.True(security.HasValidSignature(opened));
        Assert.NotNull(new SignedPrincipalExtractor(security).Extract(opened));
    }

    private static ConsumeContext OpaContext()
    {
        var env = NewEnvelope();
        return new ConsumeContext(
            env, new object(), new ServiceCollection().BuildServiceProvider(),
            null!, CancellationToken.None)
        { Source = TransportDestination.Queue("q") };
    }
}
