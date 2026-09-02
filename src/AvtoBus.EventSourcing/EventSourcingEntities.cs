using Microsoft.EntityFrameworkCore;

namespace AvtoBus.EventSourcing;

/// <summary>Готовность к персистентности: сущности для EF Core (идеи 251–300).</summary>
public sealed class EsEvent
{
    public long GlobalSequence { get; set; }
    public Guid StreamId { get; set; }
    public string StreamType { get; set; } = "";
    public int Version { get; set; }
    public string EventType { get; set; } = "";
    public int SchemaVersion { get; set; } = 1;
    public byte[] Data { get; set; } = [];
    public byte[] Metadata { get; set; } = [];
    public DateTimeOffset Timestamp { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
    public string? TenantId { get; set; }
    public string? PrevHash { get; set; }
    public string? SubjectId { get; set; }
}

public sealed class EsStream
{
    public Guid StreamId { get; set; }
    public string StreamType { get; set; } = "";
    public int Version { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public bool IsArchived { get; set; }
}

public sealed class EsSnapshot
{
    public Guid StreamId { get; set; }
    public int Version { get; set; }
    public string StateType { get; set; } = "";
    public byte[] Data { get; set; } = [];
    public DateTimeOffset CreatedAt { get; set; }
}

public sealed class EsProjectionCheckpoint
{
    public string ProjectionName { get; set; } = "";
    public long Position { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public string Status { get; set; } = "running";
    public string? LastError { get; set; }
}

/// <summary>Конфигурация EF-модели event sourcing (аналог <c>ConfigureOutbox</c>).</summary>
public static class EventSourcingModelBuilder
{
    public static ModelBuilder ConfigureEventSourcing(this ModelBuilder mb)
    {
        mb.Entity<EsEvent>(e =>
        {
            e.ToTable("avtobus_events");
            e.HasKey(x => x.GlobalSequence);
            e.HasIndex(x => new { x.StreamId, x.Version }).IsUnique();
            e.HasIndex(x => new { x.EventType, x.GlobalSequence });
            e.HasIndex(x => new { x.StreamType, x.GlobalSequence });
            e.HasIndex(x => x.SubjectId);
            e.Property(x => x.Metadata).HasColumnType("jsonb");
        });

        mb.Entity<EsStream>(e =>
        {
            e.ToTable("avtobus_streams");
            e.HasKey(x => x.StreamId);
            e.HasIndex(x => x.StreamType);
        });

        mb.Entity<EsSnapshot>(e =>
        {
            e.ToTable("avtobus_snapshots");
            e.HasKey(x => x.StreamId);
        });

        mb.Entity<EsProjectionCheckpoint>(e =>
        {
            e.ToTable("avtobus_projection_checkpoints");
            e.HasKey(x => x.ProjectionName);
        });

        return mb;
    }
}
