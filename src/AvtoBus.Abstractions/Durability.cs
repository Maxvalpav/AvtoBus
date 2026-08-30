namespace AvtoBus.Abstractions;

public enum OutboxState { Pending, Dispatching, Dispatched, Failed }

public sealed class OutboxRecord
{
    public required Guid Id { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public required string Destination { get; init; }
    public required string Transport { get; init; }
    public OutboxState State { get; set; } = OutboxState.Pending;
    public int AttemptCount { get; set; }
    public DateTimeOffset NextAttemptAt { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DispatchedAt { get; set; }
}

public interface IOutboxStore
{
    ValueTask AddAsync(IReadOnlyList<OutboxRecord> records, CancellationToken ct);

    ValueTask<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(
        int batchSize, string claimedBy, TimeSpan lockDuration, CancellationToken ct);

    ValueTask MarkDispatchedAsync(IReadOnlyList<Guid> ids, CancellationToken ct);
    ValueTask MarkFailedAttemptAsync(Guid id, string error, DateTimeOffset nextAttempt, CancellationToken ct);
    ValueTask MoveToDeadLetterAsync(Guid id, string reason, CancellationToken ct);
}

public interface IInboxStore
{
    ValueTask<bool> IsDuplicateAsync(Guid messageId, string consumerId, CancellationToken ct);
    ValueTask MarkConsumedAsync(Guid messageId, string consumerId, string messageType, CancellationToken ct);
}

/// <summary>
/// Commits all buffered store writes (outbox, inbox, saga) as a single transactional
/// outbox. Stores that write immediately (e.g. in-memory) provide a no-op implementation;
/// EF Core resolves one scoped <c>DbContext</c> so a single commit is atomic.
/// </summary>
public interface IAvtoUnitOfWork
{
    ValueTask CommitAsync(CancellationToken ct);
}

public sealed class SagaRecord
{
    public required string Id { get; init; }
    public required string SagaType { get; init; }
    public required string CorrelationId { get; init; }
    public required byte[] State { get; set; }
    public long Version { get; set; }
    public string Status { get; set; } = "Active";
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public sealed class SagaConcurrencyException(string sagaId)
    : Exception($"Optimistic concurrency conflict on saga '{sagaId}'.");

public interface ISagaStore
{
    ValueTask<SagaRecord?> LoadAsync(string sagaType, string correlationId, CancellationToken ct);
    ValueTask SaveAsync(SagaRecord record, CancellationToken ct);
    ValueTask CompleteAsync(string sagaId, CancellationToken ct);
}

public sealed class DeadLetterRecord
{
    public required Guid Id { get; init; }
    public required AvtoEnvelope Envelope { get; init; }
    public required string Reason { get; init; }
    public required string Endpoint { get; init; }
    public string? ExceptionType { get; init; }
    public int AttemptCount { get; init; }
    public DateTimeOffset FailedAt { get; init; } = DateTimeOffset.UtcNow;
}

public interface IDeadLetterStore
{
    ValueTask AddAsync(DeadLetterRecord record, CancellationToken ct);
    ValueTask<IReadOnlyList<DeadLetterRecord>> ListAsync(int limit, CancellationToken ct);
    ValueTask<DeadLetterRecord?> GetAsync(Guid id, CancellationToken ct);
    ValueTask RemoveAsync(Guid id, CancellationToken ct);
}
