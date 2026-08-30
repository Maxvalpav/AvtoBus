using System.Runtime.CompilerServices;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Outbox.EfCore;
using AvtoBus.Pipeline;
using AvtoBus.Runtime;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Tests;

/// <summary>
/// Интеграционные тесты transactional outbox / inbox на реальном PostgreSQL (док 15, док 24).
/// Доказывают: атомарность бизнес-данных и outbox, доставку relay, повторный claim после
/// краха (crash до publish), дедупликацию inbox по MessageId и SKIP LOCKED для двух relay.
/// Пропускаются, если PostgreSQL недоступен (нет AVTOBUS_PG_URL и Docker не запущен).
/// </summary>
public sealed class OutboxPostgresTests
{
    private static async Task<string> RequirePgAsync()
    {
        var cs = await PostgresTestHost.CreateDatabaseAsync();
        if (cs is null)
        {
            Assert.Skip("PostgreSQL недоступен: задайте AVTOBUS_PG_URL или запустите Docker.");
            return null!;
        }

        return cs;
    }

    private static DbContextOptions<TestOutboxContext> Options(string cs)
        => new DbContextOptionsBuilder<TestOutboxContext>().UseNpgsql(cs).Options;

    private static Envelope NewEnvelope()
        => new()
        {
            MessageId = Guid.NewGuid(),
            MessageType = "contracts.place-order",
            Body = """{"total":42}"""u8.ToArray(),
            ContentType = "application/json",
            SentAt = DateTimeOffset.UtcNow,
        };

    private static async Task EnqueueAsync(string cs, OutboxRoute route, params Envelope[] envelopes)
    {
        await using var db = new TestOutboxContext(Options(cs));
        var outbox = new EfCoreOutbox<TestOutboxContext>(
            db, new JsonEnvelopeSerializer(), new ChannelOutboxSignal(), TimeProvider.System);

        foreach (var env in envelopes)
            await outbox.EnqueueAsync(env, route, CancellationToken.None);

        await db.SaveChangesAsync(CancellationToken.None);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;

            await Task.Delay(50).ConfigureAwait(false);
        }

        return condition();
    }

    private static ServiceProvider BuildRelayServices(string cs, RecordingTransport transport, OutboxOptions? options = null)
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs));
        services.AddScoped<DbContext>(sp => sp.GetRequiredService<TestOutboxContext>());
        services.AddSingleton<IEnvelopeSerializer, JsonEnvelopeSerializer>();
        services.AddSingleton<IOutboxSignal, ChannelOutboxSignal>();
        services.AddSingleton(options ?? new OutboxOptions { PollInterval = TimeSpan.FromMilliseconds(100) });
        services.AddSingleton(new TransportRegistry([transport], transport.Name));
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton<OutboxRelay>();
        return services.BuildServiceProvider();
    }

    /// <summary>Полная шина без хоста: IMessageSession + UseOutbox для проверки атомарности session-пути.</summary>
    private static ServiceProvider BuildSessionProvider(string cs)
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs));
        services.AddScoped<DbContext>(sp => sp.GetRequiredService<TestOutboxContext>());
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddAvtoBus(bus => bus
            .UseOutbox<TestOutboxContext>()
            .AddContract<OrderPaid>());
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    [Fact]
    public async Task Outbox_and_business_data_commit_atomically()
    {
        var cs = await RequirePgAsync();
        await using var db = new TestOutboxContext(Options(cs));
        await db.Database.EnsureCreatedAsync();

        var env = NewEnvelope();

        await using var tx = await db.Database.BeginTransactionAsync();
        db.Business.Add(new BusinessRow { Name = "order-1" });
        await new EfCoreOutbox<TestOutboxContext>(
                db, new JsonEnvelopeSerializer(), new ChannelOutboxSignal(), TimeProvider.System)
            .EnqueueAsync(env, new OutboxRoute("orders", null), CancellationToken.None);
        await db.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.True(await db.Business.AnyAsync());
        var row = await db.Set<OutboxMessage>().SingleAsync(o => o.MessageId == env.MessageId);
        Assert.Null(row.SentAt);
    }

    [Fact]
    public async Task Rollback_discards_both_business_data_and_outbox_row()
    {
        var cs = await RequirePgAsync();
        await using var db = new TestOutboxContext(Options(cs));
        await db.Database.EnsureCreatedAsync();

        var env = NewEnvelope();

        await using var tx = await db.Database.BeginTransactionAsync();
        db.Business.Add(new BusinessRow { Name = "order-2" });
        await new EfCoreOutbox<TestOutboxContext>(
                db, new JsonEnvelopeSerializer(), new ChannelOutboxSignal(), TimeProvider.System)
            .EnqueueAsync(env, new OutboxRoute("orders", null), CancellationToken.None);
        await db.SaveChangesAsync();
        await tx.RollbackAsync();

        Assert.False(await db.Business.AnyAsync());
        Assert.False(await db.Set<OutboxMessage>().AnyAsync());
    }

    [Fact]
    public async Task Relay_delivers_pending_outbox_message_to_transport()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
            await db.Database.EnsureCreatedAsync();

        var env = NewEnvelope();
        await EnqueueAsync(cs, new OutboxRoute("orders", null), env);

        var transport = new RecordingTransport();
        await using var provider = BuildRelayServices(cs, transport);
        var relay = provider.GetRequiredService<OutboxRelay>();

        await relay.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(
                await WaitUntilAsync(() => transport.Contains(env.MessageId), TimeSpan.FromSeconds(10)),
                "Relay не доставил сообщение из outbox в транспорт за отведённое время.");
        }
        finally
        {
            await relay.StopAsync(CancellationToken.None);
        }

        Assert.Equal("orders", transport.DestinationOf(env.MessageId));

        await using var check = new TestOutboxContext(Options(cs));
        var row = await check.Set<OutboxMessage>().SingleAsync(o => o.MessageId == env.MessageId);
        Assert.NotNull(row.SentAt);
    }

    [Fact]
    public async Task Stale_claimed_outbox_row_is_reprocessed_after_relay_crash()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
            await db.Database.EnsureCreatedAsync();

        var env = NewEnvelope();

        // Моделируем крах relay между claim и publish: строка заклеймлена, SentAt NULL,
        // ClaimedAt старше StaleClaim — должна быть пере-claim'нута новым relay.
        await using (var db = new TestOutboxContext(Options(cs)))
        {
            db.Set<OutboxMessage>().Add(new OutboxMessage
            {
                MessageId = env.MessageId,
                Destination = "orders",
                Transport = "",
                MessageType = env.MessageType,
                EnvelopeBlob = new JsonEnvelopeSerializer().Serialize(env),
                CreatedAt = DateTime.UtcNow.AddMinutes(-20),
                ClaimedAt = DateTime.UtcNow.AddMinutes(-10),
                ClaimedBy = "dead-node/123",
            });
            await db.SaveChangesAsync();
        }

        var transport = new RecordingTransport();
        await using var provider = BuildRelayServices(
            cs, transport, new OutboxOptions { PollInterval = TimeSpan.FromMilliseconds(100), StaleClaim = TimeSpan.FromMinutes(5) });
        var relay = provider.GetRequiredService<OutboxRelay>();

        await relay.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(
                await WaitUntilAsync(() => transport.Contains(env.MessageId), TimeSpan.FromSeconds(10)),
                "Осиротевший claim не был пере-обработан новым relay.");
        }
        finally
        {
            await relay.StopAsync(CancellationToken.None);
        }

        await using var check = new TestOutboxContext(Options(cs));
        var row = await check.Set<OutboxMessage>().SingleAsync(o => o.MessageId == env.MessageId);
        Assert.NotNull(row.SentAt);
    }

    [Fact]
    public async Task Two_relays_claim_disjoint_sets_via_skip_locked()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
            await db.Database.EnsureCreatedAsync();

        var messages = Enumerable.Range(0, 20).Select(_ => NewEnvelope()).ToArray();
        await EnqueueAsync(cs, new OutboxRoute("orders", null), messages);

        var transport = new RecordingTransport();
        var options = new OutboxOptions { PollInterval = TimeSpan.FromMilliseconds(50), BatchSize = 5 };

        await using var providerA = BuildRelayServices(cs, transport, options);
        await using var providerB = BuildRelayServices(cs, transport, options);
        var relayA = providerA.GetRequiredService<OutboxRelay>();
        var relayB = providerB.GetRequiredService<OutboxRelay>();

        await relayA.StartAsync(CancellationToken.None);
        await relayB.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(
                await WaitUntilAsync(() => transport.Count >= messages.Length, TimeSpan.FromSeconds(15)),
                "Два relay не доставили все сообщения за отведённое время.");
        }
        finally
        {
            await relayA.StopAsync(CancellationToken.None);
            await relayB.StopAsync(CancellationToken.None);
        }

        var ids = transport.MessageIds;
        Assert.Equal(messages.Length, ids.Length);
        Assert.Equal(messages.Length, ids.Distinct().Count());
    }

    [Fact]
    public async Task Inbox_dedup_suppresses_duplicate_delivery_on_postgres()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
            await db.Database.EnsureCreatedAsync();

        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .Pipeline(p => p.Use<InboxDedupMiddleware>())
                .Subscribe<OrderPaid>(_ =>
                {
                    Interlocked.Increment(ref handled);
                    return Task.CompletedTask;
                }),
            services => services
                .AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs))
                .AddScoped<DbContext>(sp => sp.GetRequiredService<TestOutboxContext>())
                .AddScoped<InboxDedupMiddleware>(_ => new InboxDedupMiddleware("avtobus-postgres-test")),
            timeProvider: null);

        var messageId = Guid.NewGuid();
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });

        Assert.True(
            await harness.WaitUntilAsync(() => Volatile.Read(ref handled) >= 1, TimeSpan.FromSeconds(10)),
            "Первая доставка не обработана хендлером.");

        await Task.Delay(300);

        Assert.Equal(1, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task Inbox_dedup_namespace_is_shared_across_replicas_with_stable_group_name()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
            await db.Database.EnsureCreatedAsync();

        var messageId = Guid.NewGuid();
        var handledA = 0;
        var handledB = 0;

        // Две реплики одной группы: одинаковый consumerId («orders» = имя группы) —
        // один inbox key namespace, ключ (MessageId, ConsumerId) в общей БД.
        await using var replicaA = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .Pipeline(p => p.Use<InboxDedupMiddleware>())
                .Subscribe<OrderPaid>(_ =>
                {
                    Interlocked.Increment(ref handledA);
                    return Task.CompletedTask;
                }),
            services => services
                .AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs))
                .AddScoped<DbContext>(sp => sp.GetRequiredService<TestOutboxContext>())
                .AddScoped<InboxDedupMiddleware>(_ => new InboxDedupMiddleware("orders")),
            timeProvider: null);

        await using var replicaB = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .Pipeline(p => p.Use<InboxDedupMiddleware>())
                .Subscribe<OrderPaid>(_ =>
                {
                    Interlocked.Increment(ref handledB);
                    return Task.CompletedTask;
                }),
            services => services
                .AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs))
                .AddScoped<DbContext>(sp => sp.GetRequiredService<TestOutboxContext>())
                .AddScoped<InboxDedupMiddleware>(_ => new InboxDedupMiddleware("orders")),
            timeProvider: null);

        // Реплика A обрабатывает сообщение и фиксирует inbox-запись.
        await replicaA.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });

        // Дубликат той же доставки попадает реплике B — подавлен общим ключом (MessageId, "orders").
        await replicaB.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });

        Assert.True(
            await replicaA.WaitUntilAsync(() => Volatile.Read(ref handledA) >= 1, TimeSpan.FromSeconds(10)),
            "Реплика A не обработала сообщение.");

        await Task.Delay(500);

        Assert.Equal(1, Volatile.Read(ref handledA) + Volatile.Read(ref handledB));
    }

    [Fact]
    public async Task Inbox_cleanup_keeps_entries_newer_than_window()
    {
        var cs = await RequirePgAsync();
        await using (var db = new TestOutboxContext(Options(cs)))
        {
            await db.Database.EnsureCreatedAsync();

            var fresh = new InboxRecord
            {
                MessageId = Guid.NewGuid(),
                ConsumerId = "orders",
                ProcessedAt = DateTime.UtcNow,
            };
            var stale = new InboxRecord
            {
                MessageId = Guid.NewGuid(),
                ConsumerId = "orders",
                ProcessedAt = DateTime.UtcNow.AddHours(-2),
            };
            db.Set<InboxRecord>().AddRange(fresh, stale);
            await db.SaveChangesAsync();

            // Окно retention — 1 час: удаляются только записи старше cutoff.
            await OutboxCleanup.DeleteExpiredAsync(db, DateTime.UtcNow.AddHours(-1), CancellationToken.None);

            Assert.True(await db.Set<InboxRecord>().AnyAsync(i => i.MessageId == fresh.MessageId));
            Assert.False(await db.Set<InboxRecord>().AnyAsync(i => i.MessageId == stale.MessageId));
        }
    }

    [Fact]
    public async Task Message_session_outbox_row_commits_atomically_with_business_data()
    {
        var cs = await RequirePgAsync();
        using var provider = BuildSessionProvider(cs);
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestOutboxContext>();
        await db.Database.EnsureCreatedAsync();

        var registry = scope.ServiceProvider.GetRequiredService<MessageRegistry>();
        var session = scope.ServiceProvider.GetRequiredService<IMessageSession>();
        var orderId = Guid.NewGuid();

        // IMessageSession кладёт конверт в outbox текущей транзакции (ADR-0002):
        // публикация и бизнес-данные фиксируются одним SaveChanges.
        await session.PublishAsync(new OrderPaid(orderId));
        db.Business.Add(new BusinessRow { Name = "session-commit" });
        await db.SaveChangesAsync();

        Assert.True(await db.Business.AnyAsync(b => b.Name == "session-commit"));
        var row = await db.Set<OutboxMessage>().SingleAsync(o => o.MessageType == registry.NameOf(typeof(OrderPaid)));
        Assert.Null(row.SentAt);
        Assert.False(string.IsNullOrEmpty(row.Destination));
    }

    [Fact]
    public async Task Message_session_outbox_row_rolls_back_with_business_data()
    {
        var cs = await RequirePgAsync();
        using var provider = BuildSessionProvider(cs);
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestOutboxContext>();
        await db.Database.EnsureCreatedAsync();

        var session = scope.ServiceProvider.GetRequiredService<IMessageSession>();

        await using var tx = await db.Database.BeginTransactionAsync();
        await session.PublishAsync(new OrderPaid(Guid.NewGuid()));
        db.Business.Add(new BusinessRow { Name = "session-rollback" });
        await db.SaveChangesAsync();
        await tx.RollbackAsync();

        // Rollback отменил и бизнес-данные, и outbox-строку: сообщение не опубликовано.
        Assert.False(await db.Business.AnyAsync(b => b.Name == "session-rollback"));
        Assert.False(await db.Set<OutboxMessage>().AnyAsync());
    }

    /// <summary>
    /// B12: UseOutbox поднимает схему модуля (avtobus_outbox/avtobus_inbox/avtobus_schema_versions)
    /// при старте хоста через SchemaMigrator — без EF Migrations/EnsureCreated. Идемпотентность
    /// проверяется вторым стартом.
    /// </summary>
    [Fact]
    public async Task UseOutbox_ensures_module_schema_on_host_start()
    {
        var cs = await RequirePgAsync();

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxContext>(o => o.UseNpgsql(cs));
        builder.Services.AddAvtoBus(bus => bus
            .UseInMemory()
            .UseOutbox<TestOutboxContext>());

        using var app = builder.Build();
        await app.StartAsync();
        try
        {
            await using (var db = new TestOutboxContext(Options(cs)))
            {
                Assert.True(await TableExistsAsync(db, "avtobus_schema_versions"));
                Assert.True(await TableExistsAsync(db, "avtobus_outbox"));
                Assert.True(await TableExistsAsync(db, "avtobus_inbox"));
            }

            // Повторный старт с той же БД не падает и не дублирует схему.
            await app.StopAsync();
            await app.StartAsync();
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private static async Task<bool> TableExistsAsync(TestOutboxContext db, string table)
    {
        var connection = db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT to_regclass('public." + table + "') IS NOT NULL";
        var result = await command.ExecuteScalarAsync();
        return result is bool b && b;
    }
}

/// <summary>Бизнес-таблица для проверки атомарности с outbox.</summary>
internal sealed class BusinessRow
{
    public int Id { get; set; }

    public string Name { get; set; } = "";
}

/// <summary>DbContext тестов: бизнес-строки + стандартная outbox/inbox-модель.</summary>
internal sealed class TestOutboxContext(DbContextOptions<TestOutboxContext> options) : DbContext(options)
{
    public DbSet<BusinessRow> Business => Set<BusinessRow>();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.ConfigureOutbox();

        mb.Entity<BusinessRow>(e =>
        {
            e.ToTable("business_rows");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedOnAdd();
        });
    }
}

/// <summary>Записывает исходящие сообщения для проверки доставки relay (потокобезопасно).</summary>
internal sealed class RecordingTransport(string name = "recording") : ITransport
{
    private readonly List<(Guid MessageId, string Destination)> _sent = [];
    private readonly object _sync = new();

    public string Name { get; } = name;

    public int Count
    {
        get { lock (_sync) return _sent.Count; }
    }

    public Guid[] MessageIds
    {
        get { lock (_sync) return _sent.Select(s => s.MessageId).ToArray(); }
    }

    public bool Contains(Guid messageId)
    {
        lock (_sync) return _sent.Any(s => s.MessageId == messageId);
    }

    public string? DestinationOf(Guid messageId)
    {
        lock (_sync) return _sent.FirstOrDefault(s => s.MessageId == messageId).Destination;
    }

    public ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        lock (_sync) _sent.Add((envelope.MessageId, destination.Name));
        return ValueTask.CompletedTask;
    }

    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    public ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations, CancellationToken ct = default)
        => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
