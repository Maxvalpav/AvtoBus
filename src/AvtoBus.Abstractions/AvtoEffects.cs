using System.Collections;

namespace AvtoBus.Abstractions;

public abstract record AvtoEffect;

public sealed record PublishEffect(object Event) : AvtoEffect;
public sealed record SendEffect(object Command) : AvtoEffect;
public sealed record ReplyEffect(object Reply) : AvtoEffect;
public sealed record ScheduleEffect(object Message, TimeSpan Delay) : AvtoEffect;
public sealed record CompleteSagaEffect : AvtoEffect;

public sealed class AvtoEffects : IReadOnlyList<AvtoEffect>
{
    public static readonly AvtoEffects None = new([]);

    private readonly AvtoEffect[] _effects;

    private AvtoEffects(AvtoEffect[] effects) => _effects = effects;

    public int Count => _effects.Length;
    public AvtoEffect this[int index] => _effects[index];
    public IEnumerator<AvtoEffect> GetEnumerator()
        => ((IEnumerable<AvtoEffect>)_effects).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public static AvtoEffects Publish(object @event) => new([new PublishEffect(@event)]);
    public static AvtoEffects Send(object command) => new([new SendEffect(command)]);
    public static AvtoEffects Reply(object reply) => new([new ReplyEffect(reply)]);
    public static AvtoEffects Schedule(object message, TimeSpan delay)
        => new([new ScheduleEffect(message, delay)]);
    public static AvtoEffects CompleteSaga() => new([new CompleteSagaEffect()]);

    public static AvtoEffects All(params AvtoEffects[] batches)
    {
        var total = 0;
        foreach (var b in batches) total += b.Count;
        var merged = new AvtoEffect[total];
        var i = 0;
        foreach (var b in batches)
        {
            b._effects.CopyTo(merged, i);
            i += b.Count;
        }
        return new AvtoEffects(merged);
    }
}
