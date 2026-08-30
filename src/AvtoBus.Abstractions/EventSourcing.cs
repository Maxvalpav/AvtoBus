namespace AvtoBus.Abstractions;

/// <summary>
/// Append-only event store per 09-durability-store-contract.md
/// Primary key: (StreamName, Version)
/// </summary>
public sealed class EventStreamRecord
{
    public required string StreamName { get; init; }
    public required long Version { get; init; }
    public required Guid EnvelopeId { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public string? TenantId { get; init; }
}

public interface IEventStore
{
    /// <summary>Append events with expected version check. Throws ConcurrencyException on mismatch.</summary>
    ValueTask AppendAsync(string streamName, long expectedVersion, IReadOnlyList<object> events, CancellationToken ct);
    ValueTask<IReadOnlyList<AvtoEnvelope>> ReadAsync(string streamName, long fromVersion, int maxCount, CancellationToken ct);
    ValueTask<long> GetCurrentVersionAsync(string streamName, CancellationToken ct);
}

public sealed class ConcurrencyException(string streamName, long expected, long actual)
    : Exception($"Concurrency conflict on stream '{streamName}': expected {expected}, actual {actual}.");

// Projection checkpoint per 09
public sealed class ProjectionCheckpoint
{
    public required string ProjectionName { get; init; }
    public required string Shard { get; init; }
    public required string Position { get; init; } // e.g. "stream:version" or global offset
    public byte[]? State { get; set; }
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public interface IProjectionCheckpointStore
{
    ValueTask<ProjectionCheckpoint?> GetAsync(string projectionName, string shard, CancellationToken ct);
    ValueTask SaveAsync(ProjectionCheckpoint checkpoint, CancellationToken ct);
    ValueTask<IReadOnlyList<ProjectionCheckpoint>> ListAsync(string projectionName, CancellationToken ct);
}

// Workflow store per 09
public sealed class WorkflowInstance
{
    public required string Id { get; init; }
    public required string WorkflowType { get; init; }
    public string? TenantId { get; init; }
    public required string Status { get; set; } // Running, Completed, Failed, Cancelled, ContinuedAsNew, Faulted
    public byte[]? StateSnapshot { get; set; }
    public long Version { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
}

public sealed class WorkflowHistoryEvent
{
    public required string WorkflowId { get; init; }
    public required long Sequence { get; init; }
    public required string EventType { get; init; }
    public required byte[] Payload { get; init; }
    public string? TraceId { get; init; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
}

public interface IWorkflowStore
{
    ValueTask<WorkflowInstance?> LoadAsync(string workflowId, CancellationToken ct);
    ValueTask SaveAsync(WorkflowInstance instance, CancellationToken ct);
    ValueTask AppendHistoryAsync(IReadOnlyList<WorkflowHistoryEvent> events, CancellationToken ct);
    ValueTask<IReadOnlyList<WorkflowHistoryEvent>> ReadHistoryAsync(string workflowId, long fromSequence, CancellationToken ct);
}
