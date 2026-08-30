using System.Diagnostics.CodeAnalysis;

namespace AvtoBus.Abstractions;

public sealed class AvtoEnvelope
{
    public required Guid MessageId { get; init; }
    public required string MessageType { get; init; }
    public required string SchemaName { get; init; }
    public required int SchemaVersion { get; init; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? ConversationId { get; set; }
    public string? TenantId { get; set; }
    public string? PartitionKey { get; init; }
    public string? ReplyTo { get; set; }
    public string? TraceParent { get; set; }
    public string? TraceState { get; set; }
    public string? Source { get; set; }
    [SuppressMessage("Design", "CA1056:Uri properties should not be strings", Justification = "SchemaUri is intentionally string per CloudEvents spec")]
    public string? SchemaUri { get; set; }
    public required DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? NotBefore { get; set; }
    public DateTimeOffset? ExpiresAt { get; set; }
    public string ContentType { get; init; } = "application/json";
    public Dictionary<string, string> Headers { get; init; } = new();
    public required byte[] Body { get; init; }

    public object? Message { get; set; }

    public int DeliveryAttempt { get; set; }
}
