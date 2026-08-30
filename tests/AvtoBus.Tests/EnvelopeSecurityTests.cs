using AvtoBus;
using AvtoBus.Security;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class EnvelopeSecurityTests
{
    private static Envelope NewEnvelope(string messageType = "test.thing", string body = "{\"x\":1}")
        => new()
        {
            MessageId = Guid.NewGuid(),
            MessageType = messageType,
            Body = System.Text.Encoding.UTF8.GetBytes(body),
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
    public void Signing_protects_body_and_metadata_from_tampering()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "shared-secret";
            o.RequireSignature = true;
        });

        var envelope = NewEnvelope();
        var outgoing = security.ProtectOutbound(envelope, "orders-api");

        Assert.NotNull(outgoing.Header(EnvelopeSignerSignatureHeader));
        Assert.Equal("orders-api", outgoing.Header("avtobus-signed-by"));
        Assert.Equal(envelope.Body.ToArray(), outgoing.Body.ToArray());

        // Чистый конверт проходит проверку.
        var opened = security.OpenInbound(outgoing);
        Assert.Equal("{\"x\":1}".Length, opened.Body.Length);

        // Подмена тела ломает подпись.
        var tampered = outgoing with { Body = System.Text.Encoding.UTF8.GetBytes("{\"x\":2}") };
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(tampered));

        // Подмена MessageType тоже ломает.
        var renamed = outgoing with { MessageType = "other.type" };
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(renamed));
    }

    [Fact]
    public void Missing_signature_with_RequireSignature_is_a_violation()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "s";
            o.RequireSignature = true;
        });

        var unsigned = NewEnvelope();
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(unsigned));
    }

    [Fact]
    public void OpenInbound_returns_plain_envelope_when_security_is_disabled()
    {
        var security = Security(o => { o.MasterSecret = "s"; });

        var envelope = NewEnvelope();
        var opened = security.OpenInbound(envelope);
        Assert.Equal(envelope.Body.ToArray(), opened.Body.ToArray());
    }

    [Fact]
    public void Encryption_hides_body_and_restores_it_on_delivery()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "shared-secret";
            o.RequireSignature = true;
            o.EncryptBody = true;
        });

        var original = "секретный json {\"a\":[1,2,3]}";
        var envelope = NewEnvelope("test.thing", original);
        var outgoing = security.ProtectOutbound(envelope, "api");

        // Тело зашифровано: в транспорте нельзя прочесть полезную нагрузку.
        Assert.NotEqual(original.Length, outgoing.Body.Length);
        Assert.False(outgoing.Body.Span.SequenceEqual(System.Text.Encoding.UTF8.GetBytes(original)));
        Assert.NotNull(outgoing.Header("avtobus-encryption-nonce"));

        var opened = security.OpenInbound(outgoing);
        Assert.Equal(original, System.Text.Encoding.UTF8.GetString(opened.Body.Span));
    }

    [Fact]
    public void Encrypted_body_without_valid_nonce_is_rejected()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "shared-secret";
            o.EncryptBody = true;
            o.RequireSignature = true;
        });

        var outgoing = security.ProtectOutbound(NewEnvelope(), "api");
        var corrupted = new Dictionary<string, string>(outgoing.Headers) { ["avtobus-encryption-nonce"] = "not-base64" };
        var broken = outgoing with { Headers = corrupted };

        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(broken));
    }

    [Fact]
    public void KeyRotation_verifies_previous_generation_until_it_drops_out()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "rotating-secret";
            o.RequireSignature = true;
            o.KeyRotationInterval = TimeSpan.FromHours(1);
            o.KeepPreviousKeyGenerations = 1;
        });

        var t0 = new DateTimeOffset(2026, 1, 1, 0, 30, 0, TimeSpan.Zero);
        var t1 = t0 + TimeSpan.FromHours(1);   // эпоха 1
        var t2 = t1 + TimeSpan.FromHours(1);   // эпоха 2
        var t3 = t2 + TimeSpan.FromHours(1);   // эпоха 3 — первое поколение уже выпало

        var envelope = NewEnvelope();

        // Подписываем сообщение уже в первой после старта эпохе.
        security.RotateKeysIfDue(t1);
        var signed = security.ProtectOutbound(envelope, "svc");

        // В той же эпохе — проверка проходит.
        Assert.NotNull(security.OpenInbound(signed));

        // После ротации в эпоху 2 подпись эпохи 1 ещё валидна (KeepPrevious = 1).
        security.RotateKeysIfDue(t2);
        Assert.NotNull(security.OpenInbound(signed));

        // После ротации в эпоху 3 первое поколение выпало — нарушение.
        security.RotateKeysIfDue(t3);
        Assert.Throws<SecurityViolationException>(() => security.OpenInbound(signed));
    }

    [Fact]
    public void Outbound_rate_limit_blocks_burst_and_releases_after_a_slice()
    {
        var security = Security(o =>
        {
            o.MasterSecret = "s";
            o.OutboundRatePerSecond = 2;
        });

        var envelope = NewEnvelope();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Первые 2 проходят мгновенно.
        security.ProtectOutbound(envelope, "svc");
        security.ProtectOutbound(envelope, "svc");
        Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(800));

        // Третий внутри того же окна ждёт до начала следующей секунды.
        security.ProtectOutbound(envelope, "svc");
        stopwatch.Stop();
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(500), $"прошло {stopwatch.Elapsed}");
    }

    private const string EnvelopeSignerSignatureHeader = "avtobus-signature";

    [Fact]
    public async Task Signed_and_encrypted_message_roundtrips_through_the_bus()
    {
        var received = new TaskCompletionSource<OrderPlaced>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.UseEnvelopeSecurity(sec =>
                {
                    sec.MasterSecret = "e2e-shared-secret";
                    sec.RequireSignature = true;
                    sec.EncryptBody = true;
                });
            },
            services => services.AddSingleton(received));

        var expected = new OrderPlaced(Guid.NewGuid(), 42.5m);
        await harness.Bus.PublishAsync(expected);

        var actual = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expected, actual);
        Assert.Empty(harness.Faulted);
    }

    [Fact]
    public async Task Tampered_signature_never_reaches_the_consumer()
    {
        var received = new TaskCompletionSource<OrderPlaced>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.UseEnvelopeSecurity(sec =>
                {
                    sec.MasterSecret = "e2e-shared-secret";
                    sec.RequireSignature = true;
                });
            },
            services => services.AddSingleton(received));

        var factory = harness.Services.GetRequiredService<AvtoBus.Runtime.EnvelopeFactory>();
        var transport = harness.Transport;

        // Честно подписанный конверт, но подпись заменена на мусор.
        var envelope = factory.Create(
            new OrderPlaced(Guid.NewGuid(), 2m),
            typeof(OrderPlaced),
            messageOptions: null,
            parent: null);
        envelope = envelope.WithHeader("avtobus-signature", "AAABBBCCC");

        var destination = AvtoBus.Configuration.RoutingTable.Conventional(typeof(OrderPlaced), OutgoingKind.Publish);
        await transport.SendAsync(envelope, destination);

        // Ничего не обработалось — подпись сломана.
        await Task.Delay(300);
        Assert.DoesNotContain(harness.Consumed, m => m.Message is OrderPlaced);
    }

    [Fact]
    public async Task AddAvtoBusSecurity_di_path_wires_security_into_the_bus()
    {
        var received = new TaskCompletionSource<OrderPlaced>();

        // Безопасность подключается через DI (configureServices), а не через configurator:
        // RegisterCore должен подхватить EnvelopeSecurity из контейнера при резолве BusOptions.
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<OrderPlacedConsumer>(),
            services =>
            {
                services.AddAvtoBusSecurity(sec =>
                {
                    sec.MasterSecret = "di-shared-secret";
                    sec.RequireSignature = true;
                    sec.EncryptBody = true;
                });
                services.AddSingleton(received);
            });

        var expected = new OrderPlaced(Guid.NewGuid(), 7.25m);
        await harness.Bus.PublishAsync(expected);

        var actual = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expected, actual);
        Assert.Empty(harness.Faulted);
    }

    [Fact]
    public async Task Replayed_signed_message_is_suppressed_by_inbox()
    {
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
        {
            bus.Subscribe<OrderPaid>((_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            });
            bus.UseEnvelopeSecurity(sec =>
            {
                sec.MasterSecret = "replay-secret";
                sec.RequireSignature = true;
            });
            bus.UseInboxDeduplication(TimeSpan.FromMinutes(5));
        });

        // Replay: подписанное сообщение доставляется дважды с одним MessageId —
        // обе копии проходят проверку подписи, но Inbox подавляет повторную обработку.
        var messageId = Guid.NewGuid();
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });

        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref handled) >= 1, TimeSpan.FromSeconds(10)));
        await Task.Delay(400);

        Assert.Equal(1, Volatile.Read(ref handled));
        Assert.Empty(harness.Faulted);
    }

    [Fact]
    public async Task Wire_type_of_unregistered_clr_contract_is_poisoned_without_activation()
    {
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<OrderPaid>((_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            }));

        // Allowlist: MessageRegistry знает только подписанные контракты. Wire-тип — полное имя
        // РЕАЛЬНОГО CLR-типа (OrderPlaced), но в этом процессе он не зарегистрирован:
        // сообщение уходит в poison ДО десериализации, тип не загружается и не активируется.
        var transport = harness.Transport;
        var queue = transport.QueueDepths.Keys.Single(q => q.Contains("order-paid", StringComparison.Ordinal)
                                                           && !q.EndsWith(".error", StringComparison.Ordinal)
                                                           && !q.EndsWith(".poison", StringComparison.Ordinal));

        await transport.SendAsync(
            new Envelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = typeof(OrderPlaced).FullName!,
                Body = """{"orderId":"x","total":1}"""u8.ToArray(),
                SentAt = DateTimeOffset.UtcNow,
            },
            TransportDestination.Queue(queue));

        Assert.True(await harness.WaitForQueueDepthAsync($"{queue}.poison", 1, TimeSpan.FromSeconds(10)));
        await Task.Delay(300);

        Assert.Equal(0, Volatile.Read(ref handled));
    }
}
