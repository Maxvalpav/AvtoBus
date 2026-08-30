using System.Diagnostics.Tracing;
using AvtoBus.Observability;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Xunit;

namespace AvtoBus.Tests;

public class AvtoBusEventSourceTests
{
    private sealed class CapturingListener(string messageTypeFilter) : EventListener
    {
        private readonly object _gate = new();
        private readonly List<(int EventId, string? Message)> _written = [];

        public (int EventId, string? Message)[] Snapshot()
        {
            lock (_gate)
                return _written.ToArray();
        }

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            if (eventSource.Name == "AvtoBus-Diagnostics")
                EnableEvents(eventSource, EventLevel.Verbose);
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            // EventSource один статический: параллельные харнессы пишут свои сообщения в тот же
            // источник. Оставляем только события нашего типа-маркера, чтобы не было гонок/протечек.
            var messageType = eventData.Payload?.FirstOrDefault()?.ToString();
            if (messageType != messageTypeFilter)
                return;

            lock (_gate)
                _written.Add((eventData.EventId, eventData.Message));
        }
    }

    [Fact]
    public void Publish_consume_and_decision_produce_diagnostics_events()
    {
        using var listener = new CapturingListener("orders.place-order.v1");

        // События пишутся напрямую через статический синглтон: диагностика не зависит от шины.
        AvtoBusEventSource.Log.MessagePublished("orders.place-order.v1", "queue:orders", 64);
        AvtoBusEventSource.Log.MessageConsumed("orders.place-order.v1", "queue:orders", 12);
        AvtoBusEventSource.Log.DecisionMade("orders.place-order.v1", "00000000-0000-0000-0000-000000000001", "retry-immediate", "попытка 1");

        Assert.True(listener.Snapshot().Length == 3,
            "expected 3 diagnostics events. Got: " + string.Join(", ", listener.Snapshot().Select(w => $"#{w.EventId}({w.Message})")));
        Assert.All(listener.Snapshot(), w => Assert.NotEqual(0, w.EventId));
    }

    [Fact]
    public async Task End_to_end_publish_and_process_emit_diagnostics_events()
    {
        using var listener = new CapturingListener("contracts.order-placed");

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Subscribe<OrderPlaced>((_, _) => Task.CompletedTask));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 10m));

        Assert.True(await harness.WaitUntilAsync(
            () => listener.Snapshot().Any(w => w.EventId == 1), // MessagePublished
            TimeSpan.FromSeconds(10)),
            "no MessagePublished event. Got: " + string.Join(", ", listener.Snapshot().Select(w => $"#{w.EventId}({w.Message})")));

        Assert.True(await harness.WaitUntilAsync(
            () => listener.Snapshot().Any(w => w.EventId == 2), // MessageConsumed
            TimeSpan.FromSeconds(10)), "no MessageConsumed event");
    }
}
