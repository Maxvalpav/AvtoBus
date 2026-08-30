using System.Diagnostics;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Xunit;

namespace AvtoBus.Tests;

public class TraceDecisionTests
{
    [Fact]
    public async Task Retry_and_dead_letter_decisions_appear_as_activity_events()
    {
        var decisions = new List<string>();

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "AvtoBus",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                // ActivityListener глобальный: спаны параллельных харнессов (retry) шлют свои события.
                // Фильтруем по wire-имени нашего сообщения (тег "messaging.message.type").
                if (activity.GetTagItem("messaging.message.type") as string != "contracts.trace-tracked-event")
                    return;

                foreach (var evt in activity.Events)
                {
                    var decision = evt.Tags.FirstOrDefault(t => t.Key == "decision").Value?.ToString();
                    if (decision is not null)
                        decisions.Add(decision);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        // Две попытки ретрая и исчерпание → сообщение уходит в error-очередь.
        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(2).DelayedRetries(0))
            .Subscribe<TraceTrackedEvent>((_, _) => throw new InvalidOperationException("всегда падает")));

        await harness.Bus.PublishAsync(new TraceTrackedEvent(Guid.NewGuid()));

        Assert.True(await harness.WaitUntilAsync(
            () => harness.Transport.QueueDepths.Any(q => q.Key.EndsWith(".error", StringComparison.Ordinal) && q.Value > 0),
            TimeSpan.FromSeconds(10)));

        // Дожидаемся остановки спанов и накопления решений (при параллельной нагрузке
        // ActivityStopped может опаздывать) — ожидаем именно, а не фиксированную паузу.
        var retriesReached = await harness.WaitUntilAsync(
            () => decisions.Count(d => d == "retry-immediate") == 2,
            TimeSpan.FromSeconds(10));

        var debug = string.Join(", ", decisions);
        // Два immediate-ретрая + финальное решение в DLQ — все три должны быть в трейсе.
        Assert.True(retriesReached, "expected 2 retry-immediate. Got: [" + debug + "]");
        Assert.True(decisions.Contains("final"), "no final decision. Got: [" + debug + "]");

        listener.Dispose();
    }
}
