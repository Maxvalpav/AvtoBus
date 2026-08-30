using System.Diagnostics.Tracing;

namespace AvtoBus.Observability;

/// <summary>
/// Диагностические события .NET (идея 331): расследование инцидентов через
/// <c>dotnet-trace</c>/<c>dotnet-counters</c> без подключения APM.
/// Использование:
/// <code>
/// dotnet-trace collect --providers "AvtoBus-Diagnostics:0xFFFFFFFF:5" -- dotnet run
/// </code>
/// </summary>
[EventSource(Name = "AvtoBus-Diagnostics")]
public sealed class AvtoBusEventSource : EventSource
{
    public static readonly AvtoBusEventSource Log = new();

    private AvtoBusEventSource()
    {
    }

    private const int PublishedEventId = 1;
    private const int ConsumedEventId = 2;
    private const int FailedEventId = 3;
    private const int DecisionEventId = 4;
    private const int ReplayedEventId = 5;

    [Event(PublishedEventId, Level = EventLevel.Informational)]
    public void MessagePublished(string messageType, string destination, int bytes)
        => WriteEvent(PublishedEventId, messageType, destination, bytes);

    [Event(ConsumedEventId, Level = EventLevel.Informational)]
    public void MessageConsumed(string messageType, string source, long durationMs)
        => WriteEvent(ConsumedEventId, messageType, source, durationMs);

    [Event(FailedEventId, Level = EventLevel.Error)]
    public void MessageFailed(string messageType, string messageId, int attempt, string exception)
        => WriteEvent(FailedEventId, messageType, messageId, attempt, exception);

    [Event(DecisionEventId, Level = EventLevel.Warning)]
    public void DecisionMade(string messageType, string messageId, string decision, string reason)
        => WriteEvent(DecisionEventId, messageType, messageId, decision, reason);

    [Event(ReplayedEventId, Level = EventLevel.Informational)]
    public void MessageReplayed(string messageType, string messageId, string destination)
        => WriteEvent(ReplayedEventId, messageType, messageId, destination);

    private const int CanaryCompletedEventId = 6;
    private const int CanaryLostEventId = 7;

    [Event(CanaryCompletedEventId, Level = EventLevel.Informational)]
    public void CanaryCompleted(double rttMs)
        => WriteEvent(CanaryCompletedEventId, rttMs);

    [Event(CanaryLostEventId, Level = EventLevel.Critical)]
    public void CanaryLost(string reason)
        => WriteEvent(CanaryLostEventId, reason);

    private const int TrafficAnomalyEventId = 8;

    [Event(TrafficAnomalyEventId, Level = EventLevel.Error)]
    public void TrafficAnomaly(string messageType, string direction, long count, double ratio)
        => WriteEvent(TrafficAnomalyEventId, messageType, direction, count, ratio);

    private const int ContextTruncatedEventId = 9;

    [Event(ContextTruncatedEventId, Level = EventLevel.Warning)]
    public void ContextTruncated(string messageType, string reason)
        => WriteEvent(ContextTruncatedEventId, messageType, reason);

    private const int BlacklistedEventId = 10;

    [Event(BlacklistedEventId, Level = EventLevel.Warning)]
    public void MessageBlacklisted(string messageType, string messageId, string reason)
        => WriteEvent(BlacklistedEventId, messageType, messageId, reason);

    private const int SecurityViolationEventId = 11;

    [Event(SecurityViolationEventId, Level = EventLevel.Warning)]
    public void MessageSecurityViolation(string messageType, string reason)
        => WriteEvent(SecurityViolationEventId, messageType, reason);
}
