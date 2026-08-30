namespace AvtoBus.EventSourcing;

/// <summary>
/// Event Store: дописывание событий с optimistic concurrency, чтение стримов и глобального
/// потока (для проекций), снапшоты, архивация (идея 251).
/// </summary>
public interface IEventStore
{
    /// <summary>
    /// Дописать события в стрим с проверкой ожидаемой версии (optimistic concurrency).
    /// expectedVersion = -1 → любая; 0 → стрим должен быть новым.
    /// </summary>
    ValueTask<AppendResult> AppendAsync(
        Guid streamId,
        string streamType,
        IReadOnlyList<EventToAppend> events,
        int expectedVersion = -1,
        CancellationToken ct = default);

    /// <summary>Прочитать события стрима.</summary>
    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        Guid streamId,
        int fromVersion = 0,
        int? toVersion = null,
        CancellationToken ct = default);

    /// <summary>Прочитать глобальный поток (для проекций).</summary>
    IAsyncEnumerable<StoredEvent> ReadAllAsync(
        long fromSequence = 0,
        int batchSize = 1000,
        IReadOnlyList<string>? eventTypeFilter = null,
        CancellationToken ct = default);

    /// <summary>Прочитать события категории ($ce-orders — по streamType).</summary>
    IAsyncEnumerable<StoredEvent> ReadCategoryAsync(
        string streamType,
        long fromSequence = 0,
        CancellationToken ct = default);

    ValueTask<StreamMetadata?> GetStreamAsync(Guid streamId, CancellationToken ct = default);

    ValueTask<long> GetHeadSequenceAsync(CancellationToken ct = default);

    ValueTask SaveSnapshotAsync(StoredSnapshot snapshot, CancellationToken ct = default);

    ValueTask<StoredSnapshot?> LoadSnapshotAsync(Guid streamId, CancellationToken ct = default);

    ValueTask ArchiveStreamAsync(Guid streamId, CancellationToken ct = default);
}

public sealed record AppendResult(int NewVersion, long FirstSequence, long LastSequence)
{
    public static AppendResult Empty(int version) => new(version, 0, 0);
}

public sealed class ConcurrencyException : Exception
{
    public Guid StreamId { get; }
    public int ExpectedVersion { get; }
    public int ActualVersion { get; }

    public ConcurrencyException(Guid streamId, int expected, int actual)
        : base($"Concurrency conflict on stream {streamId}: expected v{expected}, actual v{actual}")
        => (StreamId, ExpectedVersion, ActualVersion) = (streamId, expected, actual);
}
