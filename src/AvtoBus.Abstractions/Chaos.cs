namespace AvtoBus.Abstractions;

public enum ChaosFaultKind
{
    ThrowOnAttempt,
    DelayDispatch,
    DropMessage,
    SlowHandler,
    OutboxDispatchDelay,
}

public sealed record ChaosFault(
    ChaosFaultKind Kind,
    string? Endpoint = null,
    string? Handler = null,
    int ThrowOnAttemptNumber = 2,
    TimeSpan? Delay = null,
    double Rate = 1.0,
    TimeSpan? Duration = null);

public interface IChaosInjector
{
    void Inject(ChaosFault fault);
    void Clear(string? endpoint = null);
    bool ShouldFault(string endpoint, string? handler, int attempt, out ChaosFault? fault);
    IReadOnlyList<ChaosFault> ActiveFaults { get; }
}
