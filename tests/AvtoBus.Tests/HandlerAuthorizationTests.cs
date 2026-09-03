using System.Security.Claims;
using AvtoBus;
using AvtoBus.Security;
using AvtoBus.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class HandlerAuthorizationTests
{
    private static ClaimsPrincipal Principal(params string[] roles)
    {
        var claims = roles.Select(r => new Claim(ClaimTypes.Role, r));
        var identity = new ClaimsIdentity(claims, "avtobus-test");
        return new ClaimsPrincipal(identity);
    }

    [Fact]
    public async Task Authorized_consumer_processes_message_with_matching_role()
    {
        var received = new TaskCompletionSource<AdminOnlyCommand>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<AdminOnlyConsumer>(),
            services => services.AddSingleton(received));

        using (PrincipalContext.Push(Principal("admin", "billing")))
        {
            await harness.Bus.SendAsync(new AdminOnlyCommand(Guid.NewGuid(), 10m));
        }

        var actual = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(10m, actual.Amount);
    }

    [Fact]
    public async Task Unauthorized_consumer_does_not_process_the_message()
    {
        TaskCompletionSource<AdminOnlyCommand> received = new();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.AddConsumer<AdminOnlyConsumer>();
            bus.Recoverability(r => r.ImmediateRetries(0).DelayedRetries(0));
        },
        services => services.AddSingleton(received));

        // Сообщение защищено [BusAuthorize(Roles=["admin"])], но principal не указан —
        // авторизация — внешний шаг пайплайна, RecordingMiddleware ещё не сработал.
        await harness.Bus.SendAsync(new AdminOnlyCommand(Guid.NewGuid(), 20m));

        await Task.Delay(300);
        Assert.DoesNotContain(harness.Consumed, m => m.Message is AdminOnlyCommand);
    }

    [Fact]
    public async Task Principal_is_recovered_from_envelope_on_the_consumer()
    {
        var seen = new TaskCompletionSource<ClaimsPrincipal?>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<PrincipalInspectorConsumer>(),
            services => services.AddSingleton(seen));

        using (PrincipalContext.Push(Principal("operator")))
        {
            await harness.Bus.SendAsync(new AdminOnlyCommand(Guid.NewGuid(), 33m));
        }

        var principal = await seen.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(principal);
        Assert.True(principal.Identity?.IsAuthenticated);
        Assert.True(principal.IsInRole("operator"));
    }

    [Fact]
    public async Task Principal_flow_works_e2e_with_signature_enabled()
    {
        var seen = new TaskCompletionSource<ClaimsPrincipal?>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddConsumer<PrincipalInspectorConsumer>()
                .UseEnvelopeSecurity(sec =>
                {
                    sec.MasterSecret = "auth-secret";
                    sec.RequireSignature = true;
                }),
            services => services.AddSingleton(seen));

        using (PrincipalContext.Push(Principal("operator", "admin")))
        {
            await harness.Bus.SendAsync(new AdminOnlyCommand(Guid.NewGuid(), 42m));
        }

        var principal = await seen.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(principal);
        Assert.True(principal.IsInRole("admin"));
    }

    [Fact]
    public async Task Signed_user_header_tampering_fails_signature_verification()
    {
        TaskCompletionSource<AdminOnlyCommand> received = new();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.AddConsumer<AdminOnlyConsumer>();
            bus.UseEnvelopeSecurity(sec =>
            {
                sec.MasterSecret = "auth-secret";
                sec.RequireSignature = true;
            });
        },
        services => services.AddSingleton(received));

        // Формируем конверт с честной подписью, потом подменяем пользователя вручную —
        // подпись перестаёт сходиться, сообщение уходит в poison без обработки.
        var factory = harness.Services.GetRequiredService<AvtoBus.Runtime.EnvelopeFactory>();
        var transport = harness.Transport;

        using (PrincipalContext.Push(Principal("admin")))
        {
            var envelope = await factory.CreateAsync(
                new AdminOnlyCommand(Guid.NewGuid(), 1m),
                typeof(AdminOnlyCommand),
                messageOptions: null,
                parent: null);

            Assert.NotNull(envelope.Header("avtobus-signature"));

            var attacker = new ClaimsIdentity(
                [new Claim(ClaimTypes.Role, "admin")], "attacker");
            var forged = new Dictionary<string, string>(envelope.Headers)
            {
                [BusHeaders.User] = PrincipalSerializer.Serialize(new ClaimsPrincipal(attacker))
                    ?? throw new InvalidOperationException(),
            };

            var tampered = envelope with { Headers = forged };
            await transport.SendAsync(tampered, Configuration.RoutingTable.Conventional(
                typeof(AdminOnlyCommand), OutgoingKind.Send));

            await Task.Delay(300);
            Assert.DoesNotContain(harness.Consumed, m => m.Message is AdminOnlyCommand);
        }
    }

    public sealed record AdminOnlyCommand(Guid Id, decimal Amount) : ICommand;

    [BusAuthorize(Roles = ["admin"])]
    public sealed class AdminOnlyConsumer(TaskCompletionSource<AdminOnlyCommand> received) : IConsumer<AdminOnlyCommand>
    {
        public Task ConsumeAsync(ConsumeContext<AdminOnlyCommand> context)
        {
            received.TrySetResult(context.Message);
            return Task.CompletedTask;
        }
    }

    public sealed class PrincipalInspectorConsumer(TaskCompletionSource<ClaimsPrincipal?> seen) : IConsumer<AdminOnlyCommand>
    {
        public Task ConsumeAsync(ConsumeContext<AdminOnlyCommand> context)
        {
            seen.TrySetResult(context.Principal);
            return Task.CompletedTask;
        }
    }

}

public class PrincipalSerializerTests
{
    [Fact]
    public void Roundtrip_preserves_roles_and_identity()
    {
        var principal = new ClaimsPrincipal(
            new ClaimsIdentity(
                [new Claim(ClaimTypes.Role, "admin"), new Claim(ClaimTypes.Name, "bob")],
                "avtobus-test"));

        var wire = PrincipalSerializer.Serialize(principal);
        Assert.NotNull(wire);

        var back = PrincipalSerializer.Deserialize(wire);
        Assert.NotNull(back);
        Assert.True(back.Identity?.IsAuthenticated);
        Assert.True(back.IsInRole("admin"));
        Assert.Equal("bob", back.Identity?.Name);
    }

    [Fact]
    public void Null_wire_returns_null()
    {
        Assert.Null(PrincipalSerializer.Deserialize(null));
    }

    [Fact]
    public void Garbage_wire_returns_null_instead_of_throwing()
    {
        Assert.Null(PrincipalSerializer.Deserialize("!!!not-base64!!!"));
    }
}
