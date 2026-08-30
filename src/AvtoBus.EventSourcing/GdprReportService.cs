namespace AvtoBus.EventSourcing;

/// <summary>
/// GDPR-отчёт по субъекту (идея 287): все события стора, содержащие данные субъекта.
/// Сканирование идёт по полю-субъекту (открыто), ключ для чтения PII не требуется.
/// </summary>
public interface IGdprReportService
{
    ValueTask<SubjectReport> BuildReportAsync(
        string subjectId, CancellationToken ct = default);
}

public sealed record SubjectReport(
    string SubjectId,
    IReadOnlyList<SubjectEventOccurrence> Events,
    IReadOnlyList<SubjectEventOccurrence> Forgotten);

/// <summary>Экземпляр события, содержащего данные субъекта.</summary>
public sealed record SubjectEventOccurrence(
    string EventType,
    long GlobalSequence,
    Guid StreamId,
    int Version,
    DateTimeOffset Timestamp,
    bool PiiReadable);

/// <summary>
/// Реализация: читает глобальный поток и выбирает события, чей subjectId совпадает.
/// <c>PiiReadable=false</c> — ключ субъекта удалён, зашифрованные поля недоступны.
/// </summary>
public sealed class GdprReportService : IGdprReportService
{
    private readonly IEventStore _store;
    private readonly SubjectDataProtection _protection;
    private readonly ISubjectKeyRing _keys;

    public GdprReportService(
        IEventStore store,
        SubjectDataProtection protection,
        ISubjectKeyRing keys)
    {
        _store = store;
        _protection = protection;
        _keys = keys;
    }

    public async ValueTask<SubjectReport> BuildReportAsync(
        string subjectId, CancellationToken ct = default)
    {
        var found = new List<SubjectEventOccurrence>();
        var forgotten = new List<SubjectEventOccurrence>();

        long cursor = 0;
        while (true)
        {
            var batch = new List<StoredEvent>();
            await foreach (var stored in _store.ReadAllAsync(cursor, 1000, ct: ct))
                batch.Add(stored);

            if (batch.Count == 0) break;

            foreach (var stored in batch)
            {
                if (!_protection.TryGetSubjectId(stored.EventType, stored.Data, out var storedSubject))
                    continue;

                if (!string.Equals(storedSubject, subjectId, StringComparison.Ordinal))
                    continue;

                var occurrence = new SubjectEventOccurrence(
                    stored.EventType,
                    stored.GlobalSequence,
                    stored.StreamId,
                    stored.Version,
                    stored.Timestamp,
                    PiiReadable: !_keys.IsForgotten(subjectId));

                (occurrence.PiiReadable ? found : forgotten).Add(occurrence);
            }

            cursor = batch[^1].GlobalSequence;
            if (batch.Count < 1000) break;
        }

        return new SubjectReport(subjectId, found, forgotten);
    }
}
