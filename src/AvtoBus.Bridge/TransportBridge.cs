using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Bridge;

public sealed class TransportBridge : BackgroundService
{
    private readonly AvtoBus.Runtime.TransportRegistry _registry;
    private readonly ILogger<TransportBridge> _log;
    private readonly BridgeOptions _options;

    public TransportBridge(AvtoBus.Runtime.TransportRegistry registry, ILogger<TransportBridge> log, BridgeOptions options)
    {
        _registry = registry;
        _log = log;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tasks = _options.Rules.Select(r => SupervisorLoop(r, ct)).ToList();
        await Task.WhenAll(tasks);
    }

    private async Task SupervisorLoop(BridgeRule rule, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await BridgeLoop(rule, ct);
                break;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "[Bridge] loop failed for {Source}->{Dest}, restarting in 5s", rule.SourceTransport, rule.DestinationTransport);
                try { await Task.Delay(TimeSpan.FromSeconds(5), ct); } catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task BridgeLoop(BridgeRule rule, CancellationToken ct)
    {
        var source = _registry.Get(rule.SourceTransport);
        var dest = _registry.Get(rule.DestinationTransport);
        _log.LogInformation("[Bridge] {Source} -> {Dest} pattern={Pattern}", rule.SourceTransport, rule.DestinationTransport, rule.TopicPattern ?? "*");
        var sub = new TransportSubscription(TransportDestination.Topic(rule.TopicPattern ?? "#"), ConsumerGroup: $"bridge-{rule.SourceTransport}-{rule.DestinationTransport}");
        await foreach (var msg in source.ReceiveAsync(sub, ct))
        {
            if (rule.TopicPattern != null && !Matches(rule.TopicPattern, msg.Envelope.MessageType))
            {
                // Filter mismatch: ack and skip (intentionally filtered, not lost)
                _log.LogDebug("[Bridge] skip {Type} not matching {Pattern}", msg.Envelope.MessageType, rule.TopicPattern);
                await msg.AcknowledgeAsync(ct);
                continue;
            }
            try
            {
                var destTopic = rule.DestinationTopic ?? msg.Envelope.MessageType;
                await dest.SendAsync(msg.Envelope, TransportDestination.Topic(destTopic), ct);
                await msg.AcknowledgeAsync(ct);
                _log.LogDebug("[Bridge] forwarded {Type} {Id}", msg.Envelope.MessageType, msg.Envelope.MessageId);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "[Bridge] forward failed {Id} attempt {Attempt}", msg.Envelope.MessageId, msg.Envelope.DeliveryAttempt);
                if (msg.Envelope.DeliveryAttempt >= 10)
                {
                    await msg.RejectAsync(false, ct);
                    _log.LogWarning("[Bridge] poison {Id} dead-lettered after 10 attempts", msg.Envelope.MessageId);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, Math.Min(msg.Envelope.DeliveryAttempt, 5))), ct).ConfigureAwait(false);
                    await msg.RejectAsync(true, ct);
                }
            }
        }
    }

    private static bool Matches(string pattern, string messageType)
    {
        if (pattern == "*" || pattern == "#") return true;
        if (pattern.EndsWith("*")) return messageType.StartsWith(pattern[..^1], StringComparison.Ordinal);
        return messageType == pattern;
    }
}

public sealed class BridgeOptions
{
    private readonly List<BridgeRule> _rules = [];
    public IReadOnlyList<BridgeRule> Rules => _rules;
    public BridgeOptions Map(string sourceTransport, string destTransport, string? topicPattern = null, string? destTopic = null)
    {
        _rules.Add(new BridgeRule(sourceTransport, destTransport, topicPattern, destTopic));
        return this;
    }
}

public sealed record BridgeRule(string SourceTransport, string DestinationTransport, string? TopicPattern, string? DestinationTopic);

public static class BridgeBusExtensions
{
    public static BusConfigurator UseBridge(this BusConfigurator bus, Action<BridgeOptions> configure)
    {
        var opts = new BridgeOptions();
        configure(opts);
        bus.Services.AddSingleton(opts);
        bus.Services.AddHostedService<TransportBridge>();
        return bus;
    }
}
