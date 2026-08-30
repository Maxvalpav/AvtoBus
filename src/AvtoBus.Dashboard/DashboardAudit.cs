using System.Collections.Concurrent;

namespace AvtoBus.Dashboard;

/// <summary>Строка аудита дашборда: кто, что и над чем сделал (идея 482).</summary>
public sealed record DashboardAuditRow(
    DateTimeOffset At,
    string User,
    string Action,
    string Target,
    string Detail);

/// <summary>
/// Журнал действий оператора дашборда. Опасные операции (replay, delete, pause)
/// обязаны писать сюда строку — без аудита они не выполняются.
/// </summary>
public interface IDashboardAuditLog
{
    IReadOnlyCollection<DashboardAuditRow> Rows { get; }

    void Write(DashboardAuditRow row);
}

/// <summary>In-memory журнал аудита: для монолита и тестов. Производственные развороты
/// подключают собственную реализацию (EF Core, база аудита).</summary>
public sealed class InMemoryDashboardAuditLog : IDashboardAuditLog
{
    private const int MaxRows = 10_000;
    private readonly ConcurrentQueue<DashboardAuditRow> _rows = new();

    public IReadOnlyCollection<DashboardAuditRow> Rows => _rows.ToArray();

    public void Write(DashboardAuditRow row)
    {
        _rows.Enqueue(row);
        while (_rows.Count > MaxRows)
            _rows.TryDequeue(out _);
    }
}
