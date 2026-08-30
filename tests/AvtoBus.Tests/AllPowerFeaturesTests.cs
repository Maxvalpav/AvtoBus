using AvtoBus;
using AvtoBus.Canvas;
using AvtoBus.Mongo;
using AvtoBus.Observability;
using AvtoBus.Pipeline;
using AvtoBus.Runtime;
using AvtoBus.Security;
using AvtoBus.Streams;
using AvtoBus.Workflow;
using AvtoBus.Actors;
using AvtoBus.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace AvtoBus.Tests;

public class UniqueJobsTests
{
    [Fact]
    public void Unique_store_prevents_duplicate_within_ttl()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryUniqueStore(time);
        var key = "order:123";
        Assert.True(store.TryAcquire(key, TimeSpan.FromSeconds(30)));
        Assert.False(store.TryAcquire(key, TimeSpan.FromSeconds(30)));
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.True(store.TryAcquire(key, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void Unique_key_computer_hashes_body()
    {
        var attr = new UniqueJobAttribute { ByArgs = true, ByQueue = true };
        var k1 = UniqueKeyComputer.Compute(new { Id = 1 }, typeof(object), "q", attr);
        var k2 = UniqueKeyComputer.Compute(new { Id = 2 }, typeof(object), "q", attr);
        Assert.NotEqual(k1, k2);
    }
}

public class ThrottleTests
{
    [Fact]
    public async Task Throttle_defers_when_over_limit()
    {
        var mw = new ThrottleMiddleware(1, TimeSpan.FromSeconds(10), new FakeTimeProvider());
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None) { Source = TransportDestination.Queue("q") };
        var called = 0;
        BusDelegate next = _ => { called++; return ValueTask.CompletedTask; };
        await mw.InvokeAsync(ctx, next);
        Assert.Equal(1, called);
        var ctx2 = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None) { Source = TransportDestination.Queue("q") };
        await mw.InvokeAsync(ctx2, next);
        Assert.Equal(1, called); // second throttled, not called
        Assert.Equal(ConsumeOutcome.Deferred, ctx2.Outcome);
    }
}

public class CanvasTests
{
    [Fact]
    public async Task Canvas_chain_dispatches_first_message()
    {
        var services = new ServiceCollection();
        services.AddAvtoBus(bus => bus.UseTransport(new AvtoBus.InMemory.InMemoryTransport()));
        using var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IBus>();
        var chain = new CanvasChain().Add(new TestMsg("a")).Add(new TestMsg("b"));
        Assert.Equal(2, chain.Steps.Count);
        // Dispatch should not throw (bus is real)
        await chain.DispatchAsync(bus);
    }
    private sealed record TestMsg(string V);
}

public class WorkflowWaitTests
{
    [Fact]
    public async Task Workflow_sleep_until_uses_timer()
    {
        var store = new InMemoryWorkflowStore();
        var time = new FakeTimeProvider();
        var runner = new WorkflowInstanceRunner(store, time);
        var id = await runner.StartAsync("test", new { }, CancellationToken.None);
        var ctx = runner.CreateContext(id);
        await ctx.SleepUntil(time.GetUtcNow().AddMinutes(5));
        var hist = await store.ReadHistoryAsync(id, 0, CancellationToken.None);
        Assert.Contains(hist, h => h.EventType == "TimerCreated");
    }

    private sealed class InMemoryWorkflowStore : IWorkflowStore
    {
        private readonly Dictionary<string, List<WorkflowHistoryEvent>> _h = new();
        public ValueTask SaveAsync(WorkflowInstance i, CancellationToken ct) { _h[i.Id] = []; return ValueTask.CompletedTask; }
        public ValueTask<WorkflowInstance?> LoadAsync(string id, CancellationToken ct) => ValueTask.FromResult<WorkflowInstance?>(null);
        public ValueTask AppendHistoryAsync(IReadOnlyList<WorkflowHistoryEvent> ev, CancellationToken ct) { foreach (var e in ev) { if (!_h.ContainsKey(e.WorkflowId)) _h[e.WorkflowId] = []; _h[e.WorkflowId].Add(e); } return ValueTask.CompletedTask; }
        public ValueTask<IReadOnlyList<WorkflowHistoryEvent>> ReadHistoryAsync(string id, long from, CancellationToken ct) => ValueTask.FromResult((IReadOnlyList<WorkflowHistoryEvent>)(_h.TryGetValue(id, out var l) ? l : Array.Empty<WorkflowHistoryEvent>()));
    }
}

public class StreamsTests
{
    [Fact]
    public void Session_window_builds_sessions()
    {
        var w = new SessionWindow<int>(TimeSpan.FromMinutes(5));
        var now = DateTimeOffset.UtcNow;
        var recs = new List<StreamRecord<string, int>>
        {
            new("k",1,now),
            new("k",2,now.AddMinutes(1)),
            new("k",3,now.AddMinutes(10)),
        };
        var sessions = w.Build(recs);
        Assert.Equal(2, sessions.Count);
        Assert.Equal(2, sessions[0].Values.Count);
        Assert.Single(sessions[1].Values);
    }

    [Fact]
    public async Task Stream_join_returns_joined()
    {
        var store = new InMemoryStateStore<string, string>();
        await store.PutAsync("k", "right", CancellationToken.None);
        var join = new StreamJoinProcessor<string, string, string>(store, TimeSpan.FromMinutes(5), (l, r) => l + r);
        async IAsyncEnumerable<StreamRecord<string, string>> Input()
        {
            yield return new StreamRecord<string, string>("k", "left", DateTimeOffset.UtcNow);
            await Task.CompletedTask;
        }
        var outList = new List<StreamRecord<string, string>>();
        await foreach (var r in join.ProcessAsync(Input(), CancellationToken.None)) outList.Add(r);
        Assert.Single(outList);
        Assert.Equal("leftright", outList[0].Value);
    }
}

public class DebeziumTests
{
    [Fact]
    public async Task Cdc_reader_enqueues_and_reads()
    {
        var reader = new InMemoryCdcReader();
        reader.Enqueue(new CdcOutboxRow(Guid.NewGuid(), "a", new byte[] { 1 }, "q", "inmemory", 1, DateTimeOffset.UtcNow));
        var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var count = 0;
        await foreach (var _ in reader.ReadAsync(cts.Token)) { count++; break; }
        Assert.Equal(1, count);
    }
}

public class BloblangTests
{
    [Fact]
    public async Task Bloblang_middleware_passes_through()
    {
        var mw = new BloblangMiddleware(new BloblangOptions { Mapping = "root.foo = this.foo" });
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new { foo = 1 }, new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None) { Source = TransportDestination.Queue("q") };
        var called = false;
        await mw.InvokeAsync(ctx, _ => { called = true; return ValueTask.CompletedTask; });
        Assert.True(called);
        Assert.Equal("root.foo = this.foo", ctx.Items["avtobus.bloblang.applied"]);
    }
}

public class WasmTests
{
    [Fact]
    public async Task Wasm_filter_skips_when_null()
    {
        var wasm = new ManagedWasmTransform(new WasmOptions { ManagedFallback = _ => null });
        var mw = new WasmMiddleware(wasm);
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = new byte[] { 1 }, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None) { Source = TransportDestination.Queue("q") };
        await mw.InvokeAsync(ctx, _ => ValueTask.CompletedTask);
        Assert.Equal(ConsumeOutcome.Skipped, ctx.Outcome);
    }
}

public class HangfireTests
{
    [Fact]
    public void Hangfire_job_envelope_has_ids()
    {
        var job = new AvtoBus.Hangfire.HangfireJobEnvelope { JobId = "1", TypeName = typeof(object).AssemblyQualifiedName!, MethodName = "ToString", EnqueuedAt = DateTimeOffset.UtcNow };
        Assert.Equal("1", job.JobId);
    }
}

public class FlinkSqlTests
{
    [Fact]
    public void Flink_sql_compiles_window()
    {
        var store = new InMemoryStateStore<string, int>();
        var topo = new SqlStreamTopology<int>("SELECT * FROM s WINDOW TUMBLING(1m)");
        var agg = topo.CompileWindow(list => list.Sum());
        Assert.Equal(TimeSpan.FromMinutes(1), agg.WindowSize);
    }
}

public class RetryScheduleTests
{
    [Fact]
    public void Retry_schedule_exponential_jitter()
    {
        var s = RetrySchedule.Exponential(TimeSpan.FromMilliseconds(100)).Jitter(0.2);
        var d = s.NextDelay(1, new Exception());
        Assert.NotNull(d);
        Assert.InRange(d.Value.TotalMilliseconds, 80, 120);
    }
}

public class OpaTests
{
    [Fact]
    public void Opa_evaluator_allows_tenant()
    {
        var eval = new RegoEvaluator();
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow, TenantId = "eu" };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None) { Source = TransportDestination.Queue("q") };
        Assert.True(eval.IsAllowed(ctx, "input.tenant == \"eu\""));
        Assert.False(eval.IsAllowed(ctx, "input.tenant == \"us\""));
    }
}

public class ActorsTests
{
    [Fact]
    public async Task Virtual_actor_store_roundtrips()
    {
        var store = new InMemoryActorStore<TestState>();
        var id = new ActorId("a1");
        await store.SaveAsync(id, new TestState { Value = 42 }, CancellationToken.None);
        var loaded = await store.LoadAsync(id, CancellationToken.None);
        Assert.Equal(42, loaded!.Value);
    }
    private sealed class TestState : IEquatable<TestState> { public int Value { get; set; } public bool Equals(TestState? o) => o?.Value == Value; }
}

public class ToxiproxyTests
{
    [Fact]
    public async Task Toxiproxy_down_throws()
    {
        var inner = new AvtoBus.InMemory.InMemoryTransport();
        var opts = new AvtoBus.Runtime.ToxiproxyOptions { DownProbability = 1.0 };
        var tox = new AvtoBus.Runtime.ToxiproxyTransport(inner, opts);
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        await Assert.ThrowsAsync<IOException>(() => tox.SendAsync(env, TransportDestination.Queue("q")).AsTask());
    }
}
