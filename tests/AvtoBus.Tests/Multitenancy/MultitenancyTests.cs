using AvtoBus;
using AvtoBus.Multitenancy;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace AvtoBus.Tests.Multitenancy;

public class MultitenancyTests
{
    private static TenantRegistry Registry(Action<TenantOptions> configure)
    {
        var options = new TenantOptions();
        configure(options);
        return new TenantRegistry(options);
    }

    // ---- TenantContext (идея 461) --------------------------------------

    [Fact]
    public void TenantContext_push_pop_restores_previous_tenant()
    {
        using var outer = TenantContext.Push("acme");
        Assert.Equal("acme", TenantContext.Get());

        using (TenantContext.Push("globex"))
        {
            Assert.Equal("globex", TenantContext.Get());
        }

        Assert.Equal("acme", TenantContext.Get());
    }

    [Fact]
    public void TenantContext_push_null_clears_tenant()
    {
        using var outer = TenantContext.Push("acme");
        using (TenantContext.Push(null))
        {
            Assert.Null(TenantContext.Get());
        }

        Assert.Equal("acme", TenantContext.Get());
    }

    [Fact]
    public async Task TenantContext_flows_across_async_awaits()
    {
        using var _ = TenantContext.Push("acme");

        // AsyncLocal: значение живёт в контексте исполнения, переживает await.
        await Task.Yield();
        Assert.Equal("acme", TenantContext.Get());
    }

    // ---- TenantRegistry (идеи 461, 464, 467) ----------------------------

    [Fact]
    public void RegionOf_returns_configured_region_or_null()
    {
        var registry = Registry(o =>
        {
            o.AddTenant("acme", t => t.Region = "eu");
            o.AddTenant("globex");
        });

        Assert.Equal("eu", registry.RegionOf("acme"));
        Assert.Null(registry.RegionOf("globex"));
        Assert.Null(registry.RegionOf("unknown"));
    }

    [Fact]
    public void IsolationOf_falls_back_to_global_level()
    {
        var registry = Registry(o =>
        {
            o.Isolation = TenantIsolation.QueuePerTenant;
            o.AddTenant("acme", t => t.Isolation = TenantIsolation.Shared);
            o.AddTenant("globex");
        });

        Assert.Equal(TenantIsolation.Shared, registry.IsolationOf("acme"));
        Assert.Equal(TenantIsolation.QueuePerTenant, registry.IsolationOf("globex"));
        Assert.Equal(TenantIsolation.QueuePerTenant, registry.IsolationOf("unknown"));
    }

    [Fact]
    public void InboundRateOf_returns_quota_or_zero_for_unlimited()
    {
        var registry = Registry(o =>
        {
            o.AddTenant("acme", t => t.InboundRatePerSecond = 500);
        });

        Assert.Equal(500, registry.InboundRateOf("acme"));
        Assert.Equal(0, registry.InboundRateOf("unknown"));
    }

    // ---- RegionRouteGuard (идея 467) ------------------------------------

    [Fact]
    public void Guard_allows_tenant_in_current_region()
    {
        var registry = Registry(o =>
        {
            o.AddTenant("acme", t => t.Region = "eu");
        });
        var options = new TenantOptions { CurrentRegion = "eu" };
        var guard = new RegionRouteGuard(registry, options);

        // Сервис в EU публикует данные EU-тенанта — разрешено.
        var envelope = NewEnvelope(tenantId: "acme");
        guard.Validate(envelope, AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish));
    }

    [Fact]
    public void Guard_blocks_tenant_in_foreign_region()
    {
        var registry = Registry(o =>
        {
            o.AddTenant("acme", t => t.Region = "eu");
        });
        var options = new TenantOptions { CurrentRegion = "us" };
        var guard = new RegionRouteGuard(registry, options);

        var envelope = NewEnvelope(tenantId: "acme");
        Assert.Throws<RegionViolationException>(() =>
            guard.Validate(envelope, AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish)));
    }

    [Fact]
    public void Guard_allows_tenant_without_region()
    {
        var registry = Registry(o => o.AddTenant("acme"));
        var options = new TenantOptions { CurrentRegion = "us" };
        var guard = new RegionRouteGuard(registry, options);

        var envelope = NewEnvelope(tenantId: "acme");
        guard.Validate(envelope, AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish));
    }

    [Fact]
    public void Guard_allows_cross_region_when_explicitly_permitted()
    {
        var registry = Registry(o =>
        {
            o.AllowCrossRegion = true;
            o.AddTenant("acme", t => t.Region = "eu");
        });
        var options = new TenantOptions { CurrentRegion = "us" };
        var guard = new RegionRouteGuard(registry, options);

        var envelope = NewEnvelope(tenantId: "acme");
        guard.Validate(envelope, AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish));
    }

    [Fact]
    public void Guard_ignores_messages_without_tenant()
    {
        var registry = Registry(o => o.AddTenant("acme", t => t.Region = "eu"));
        var options = new TenantOptions { CurrentRegion = "us" };
        var guard = new RegionRouteGuard(registry, options);

        var envelope = NewEnvelope(tenantId: null);
        guard.Validate(envelope, AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish));
    }

    [Fact]
    public void Region_and_GeoReplicated_attributes_carry_metadata()
    {
        Assert.Equal("eu", typeof(EuOrderPlaced).GetCustomAttributesData()
            .First(a => a.AttributeType == typeof(RegionAttribute))
            .ConstructorArguments[0].Value);

        Assert.NotNull(typeof(EuOrderPlaced).GetCustomAttributes(typeof(GeoReplicatedAttribute), false).FirstOrDefault());
    }

    // ---- TenantRateLimitMiddleware (идея 464) ---------------------------

    [Fact]
    public async Task Rate_limit_defers_messages_over_the_quota()
    {
        var registry = Registry(o => o.AddTenant("acme", t => t.InboundRatePerSecond = 1));
        var middleware = new TenantRateLimitMiddleware(registry, TimeProvider.System);

        var first = CreateContext(tenantId: "acme");
        var second = CreateContext(tenantId: "acme");

        // Первое сообщение в секунде проходит, второе — откладывается (backpressure).
        await middleware.InvokeAsync(first, _ => default);
        Assert.Equal(ConsumeOutcome.Handled, first.Outcome);

        await middleware.InvokeAsync(second, _ => default);
        Assert.Equal(ConsumeOutcome.Deferred, second.Outcome);
        Assert.NotNull(second.DeferralDelay);
    }

    [Fact]
    public async Task Rate_limit_allows_other_tenants_unaffected()
    {
        var registry = Registry(o => o.AddTenant("acme", t => t.InboundRatePerSecond = 1));
        var middleware = new TenantRateLimitMiddleware(registry, TimeProvider.System);

        var acme = CreateContext(tenantId: "acme");
        var globex = CreateContext(tenantId: "globex");

        await middleware.InvokeAsync(acme, _ => default);
        await middleware.InvokeAsync(globex, _ => default);

        Assert.Equal(ConsumeOutcome.Handled, globex.Outcome);
    }

    [Fact]
    public async Task Rate_limit_passes_untagged_messages()
    {
        var registry = Registry(o => o.AddTenant("acme", t => t.InboundRatePerSecond = 1));
        var middleware = new TenantRateLimitMiddleware(registry, TimeProvider.System);

        var context = CreateContext(tenantId: null);
        await middleware.InvokeAsync(context, _ => default);
        Assert.Equal(ConsumeOutcome.Handled, context.Outcome);
    }

    // ---- TenantIsolationPolicy (идея 462, уровни B/C) -------------------

    [Fact]
    public void Queue_per_tenant_appends_tenant_suffix_to_destination()
    {
        var policy = new TenantIsolationPolicy(Registry(o =>
        {
            o.AddTenant("acme", t => t.Isolation = TenantIsolation.QueuePerTenant);
        }));

        var dest = new TransportDestination("place-order", DestinationKind.Queue);
        Assert.Equal("place-order.acme", policy.Isolate(dest, "acme").Name);
        Assert.Equal(DestinationKind.Queue, policy.Isolate(dest, "acme").Kind);
    }

    [Fact]
    public void Namespace_per_tenant_prepends_tenant_prefix_to_destination()
    {
        var policy = new TenantIsolationPolicy(Registry(o =>
        {
            o.AddTenant("acme", t => t.Isolation = TenantIsolation.NamespacePerTenant);
        }));

        var dest = new TransportDestination("place-order", DestinationKind.Queue);
        Assert.Equal("acme.place-order", policy.Isolate(dest, "acme").Name);
    }

    [Fact]
    public void Shared_isolation_leaves_destination_unchanged()
    {
        var policy = new TenantIsolationPolicy(Registry(o =>
        {
            o.Isolation = TenantIsolation.Shared;
            o.AddTenant("acme");
        }));

        var dest = new TransportDestination("place-order", DestinationKind.Queue);
        Assert.Equal(dest, policy.Isolate(dest, "acme"));
        Assert.Equal(dest, policy.Isolate(dest, "unknown"));
    }

    [Fact]
    public async Task Queue_per_tenant_isolation_routes_messages_to_tenant_queues()
    {
        // Уровень B: у каждого тенанта своя физическая очередь. Сообщение тенанта acme
        // обязано быть вычитано из очереди acme, а не из общей или чужой.
        var seen = new System.Collections.Concurrent.ConcurrentQueue<(string Tenant, string Queue)>();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .UseMultitenancy(o => o
                .AddTenant("acme", t => t.Isolation = TenantIsolation.QueuePerTenant)
                .AddTenant("globex", t => t.Isolation = TenantIsolation.QueuePerTenant))
            .Subscribe<PlaceOrder>(ctx =>
            {
                seen.Enqueue((ctx.Envelope.TenantId!, ctx.Source.Name));
                return Task.CompletedTask;
            }));

        await harness.Bus.SendAsync(
            new PlaceOrder(Guid.NewGuid(), "cust-1", 10),
            new SendOptions { TenantId = "acme" });
        await harness.Bus.SendAsync(
            new PlaceOrder(Guid.NewGuid(), "cust-2", 20),
            new SendOptions { TenantId = "globex" });

        Assert.True(
            await harness.WaitUntilAsync(() => seen.Count >= 2, TimeSpan.FromSeconds(10)),
            "Оба сообщения не были обработаны.");

        var byTenant = seen.ToList();
        Assert.Contains(byTenant, x => x.Tenant == "acme" && x.Queue == "place-order.acme");
        Assert.Contains(byTenant, x => x.Tenant == "globex" && x.Queue == "place-order.globex");
        Assert.DoesNotContain(byTenant, x => x.Tenant == "acme" && x.Queue != "place-order.acme");
        Assert.DoesNotContain(byTenant, x => x.Tenant == "globex" && x.Queue != "place-order.globex");
    }

    [Fact]
    public async Task Cross_tenant_storage_access_is_forbidden()
    {
        // Два сервиса на ОДНОМ брокере, каждый обслуживает своего тенанта (уровень B).
        // Хост acme читает только очередь acme: сообщение globex того же типа не должно
        // доехать до него — изоляция на уровне хранилища, а не на уровне консьюмера.
        var transport = new AvtoBus.InMemory.InMemoryTransport();
        var acmeSeen = 0;
        var globexSeen = 0;

        var acme = await StartTenantHostAsync(transport, "acme", () => Interlocked.Increment(ref acmeSeen));
        var globex = await StartTenantHostAsync(transport, "globex", () => Interlocked.Increment(ref globexSeen));
        try
        {
            var globexBus = globex.Services.GetRequiredService<IBus>();
            await globexBus.SendAsync(
                new PlaceOrder(Guid.NewGuid(), "cust-globex", 100),
                new SendOptions { TenantId = "globex" });

            await Task.Delay(500);

            Assert.Equal(0, Volatile.Read(ref acmeSeen));
            Assert.Equal(1, Volatile.Read(ref globexSeen));

            var acmeBus = acme.Services.GetRequiredService<IBus>();
            await acmeBus.SendAsync(
                new PlaceOrder(Guid.NewGuid(), "cust-acme", 5),
                new SendOptions { TenantId = "acme" });

            Assert.True(
                await WaitUntilAsync(() => Volatile.Read(ref acmeSeen) >= 1, TimeSpan.FromSeconds(10)),
                "Сообщение своего тенанта не обработано хостом acme.");
        }
        finally
        {
            await acme.StopAsync(CancellationToken.None);
            acme.Dispose();
            await globex.StopAsync(CancellationToken.None);
            globex.Dispose();
        }
    }

    private static async Task<Microsoft.Extensions.Hosting.IHost> StartTenantHostAsync(
        AvtoBus.InMemory.InMemoryTransport transport,
        string tenantId,
        Action onHandled)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddLogging();

        builder.Services.AddSingleton(transport);
        builder.Services.AddSingleton<AvtoBus.ITransport>(transport);
        builder.Services.AddSingleton<AvtoBus.Observability.IQueueDepthProvider>(transport);

        builder.Services.AddAvtoBus(bus => bus
            .UseMultitenancy(o => o.AddTenant(tenantId, t => t.Isolation = TenantIsolation.QueuePerTenant))
            .Subscribe<PlaceOrder>((_, _) =>
            {
                onHandled();
                return Task.CompletedTask;
            }));

        var host = builder.Build();
        await host.StartAsync();
        await WaitForConsumersAsync(host);
        return host;
    }

    private static async Task WaitForConsumersAsync(Microsoft.Extensions.Hosting.IHost host)
    {
        var consumerHost = host.Services.GetRequiredService<AvtoBus.Runtime.ConsumerHost>();
        for (var i = 0; i < 200 && consumerHost.Runners.Count == 0; i++)
            await Task.Delay(5);

        await Task.Delay(50);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(25);
        }

        return condition();
    }

    [Fact]
    public async Task Shared_isolation_leaves_messages_in_common_queue()
    {
        // Уровень A: общая очередь, все тенанты видят друг друга — изоляция только на консьюмере.
        var seen = new System.Collections.Concurrent.ConcurrentQueue<string>();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .UseMultitenancy(o => o.AddTenant("acme"))
            .Subscribe<PlaceOrder>(ctx =>
            {
                seen.Enqueue(ctx.Source.Name);
                return Task.CompletedTask;
            }));

        await harness.Bus.SendAsync(
            new PlaceOrder(Guid.NewGuid(), "cust-1", 1),
            new SendOptions { TenantId = "acme" });

        Assert.True(
            await harness.WaitUntilAsync(() => seen.Count >= 1, TimeSpan.FromSeconds(10)),
            "Сообщение в общей очереди не обработано.");

        Assert.Contains("place-order", seen);
    }

    // ---- helpers --------------------------------------------------------

    private static Envelope NewEnvelope(string? tenantId)
        => new()
        {
            MessageId = Guid.NewGuid(),
            MessageType = "contracts.order-placed",
            Body = System.Text.Encoding.UTF8.GetBytes("{}"),
            ContentType = "application/json",
            SentAt = DateTimeOffset.UtcNow,
            TenantId = tenantId,
            Headers = new Dictionary<string, string>(),
        };

    private static ConsumeContext CreateContext(string? tenantId)
    {
        var envelope = NewEnvelope(tenantId);
        return AvtoBus.Runtime.ContextFactory.Create(
            typeof(OrderPlaced),
            envelope,
            new OrderPlaced(Guid.NewGuid(), 1m),
            new Microsoft.Extensions.DependencyInjection.ServiceCollection().BuildServiceProvider(),
            bus: null!,
            CancellationToken.None,
            new AvtoBus.TransportDestination("test", AvtoBus.DestinationKind.Queue));
    }
}

/// <summary>Контракт с привязкой к региону и репликацией (идеи 467, 473).</summary>
[Region("eu")]
[GeoReplicated]
public sealed record EuOrderPlaced(Guid OrderId, decimal Amount);
