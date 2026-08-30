namespace AvtoBus.EventSourcing;

/// <summary>Событие в сторе (идеи 251–300 Event Sourcing).</summary>
public sealed record StoredEvent
{
    public required long GlobalSequence { get; init; }
    public required Guid StreamId { get; init; }
    public required string StreamType { get; init; }
    public required int Version { get; init; }
    public required string EventType { get; init; }
    public required int SchemaVersion { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }
    public ReadOnlyMemory<byte> Metadata { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public Guid? CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public string? TenantId { get; init; }
    public string? PrevHash { get; init; }
}

/// <summary>Событие для записи (ещё без GlobalSequence).</summary>
public sealed record EventToAppend
{
    public required object Payload { get; init; }
    public required string EventType { get; init; }
    public int SchemaVersion { get; init; } = 1;
    public Dictionary<string, string> Metadata { get; init; } = new();
}

/// <summary>Снапшот состояния агрегата.</summary>
public sealed record StoredSnapshot
{
    public required Guid StreamId { get; init; }
    public required int Version { get; init; }
    public required string StateType { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
}

public sealed record StreamMetadata(
    Guid StreamId,
    string StreamType,
    int Version,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    bool IsArchived);
