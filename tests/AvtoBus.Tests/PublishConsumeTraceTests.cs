using System.Collections.Concurrent;
using System.Diagnostics;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>
/// Сквозной трейс publish → consume (идея 301): consume-спан лежит в том же трейсе,
/// что и publish, и ссылается на publish-спан как на родителя через traceparent.
/// </summary>
public class PublishConsumeTraceTests
{
    [Fact]
    public async Task Consume_span_is_child_of_publish_span_in_the_same_trace()
    {
        // TraceId -> SpanId опубликованных сообщений.
        var publishSpans = new ConcurrentDictionary<string, string>();
        var consumeSpans = new ConcurrentBag<(string TraceId, string ParentSpanId)>();

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "AvtoBus",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.GetTagItem("messaging.message.type") as string != "contracts.trace-flow-event")
                    return;

                switch (activity.GetTagItem("messaging.operation") as string)
                {
                    case "publish":
                        publishSpans[activity.TraceId.ToString()] = activity.SpanId.ToString();
                        break;
                    case "process":
                        consumeSpans.Add((activity.TraceId.ToString(), activity.ParentSpanId.ToString()));
                        break;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.Subscribe<TraceFlowEvent>((_, _) => Task.CompletedTask));

        await harness.Bus.PublishAsync(new TraceFlowEvent(Guid.NewGuid()));
        Assert.True(await harness.WaitForConsumedAsync<TraceFlowEvent>());

        // ActivityStopped может опаздывать за фактическим завершением спана — ждём оба.
        Assert.True(await harness.WaitUntilAsync(
            () => publishSpans.Count == 1 && consumeSpans.Count == 1,
            TimeSpan.FromSeconds(5)));

        var consume = Assert.Single(consumeSpans);

        // Consume-спан в том же трейсе, что и publish, и его родитель — publish-спан.
        Assert.True(publishSpans.TryGetValue(consume.TraceId, out var publishSpanId));
        Assert.Equal(publishSpanId, consume.ParentSpanId);
    }
}
