namespace AvtoBus.Abstractions;

public sealed record AvtoHandlerOutcome(
    AvtoHandlerStatus Status,
    AvtoEffects Effects,
    string? StopReason = null)
{
    public static AvtoHandlerOutcome Success(AvtoEffects effects) => new(AvtoHandlerStatus.Success, effects);
    public static AvtoHandlerOutcome Stopped(string reason) => new(AvtoHandlerStatus.Stopped, AvtoEffects.None, reason);
}

public enum AvtoHandlerStatus { Success, Stopped }

public sealed class AvtoInvocationContext
{
    public required AvtoEnvelope Envelope { get; init; }
    public required IServiceProvider Services { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}

public interface IAvtoHandlerInvoker
{
    string HandlerName { get; }
    Type MessageType { get; }
    ValueTask<AvtoHandlerOutcome> InvokeAsync(AvtoInvocationContext context);
}

public enum HandlerContinuation { Continue, Stop }

public sealed record ValidationResult(bool IsValid, string? Error)
{
    public static readonly ValidationResult Valid = new(true, null);
    public static ValidationResult Invalid(string error) => new(false, error);
}
