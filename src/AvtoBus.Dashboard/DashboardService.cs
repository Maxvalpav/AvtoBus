using AvtoBus.Configuration;
using AvtoBus.Observability;
using AvtoBus.Runtime;

namespace AvtoBus.Dashboard;

/// <summary>Состояние шины глазами дашборда (док 23): обзор, очереди, DLQ.</summary>
public sealed record DashboardOverview(
    string Mode,
    int TotalPending,
    int ConsumerCount,
    int DlqCount,
    IReadOnlyList<DashboardQueue> Queues);

/// <summary>Очередь: глубина, консьюмеры, признак DLQ.</summary>
public sealed record DashboardQueue(
    string Name,
    int Messages,
    int Consumers,
    bool IsDlq);

/// <summary>
/// Read-модель дашборда поверх уже существующей инфраструктуры: глубины очередей
/// (<see cref="IQueueDepthProvider"/>), консьюмеры (<see cref="ConsumerHost"/>) и
/// DLQ (<see cref="DlqReader"/>). Опасные операции выполняются только через
/// <see cref="IDashboardAuditLog"/> и отключаемы в проде (идея 482).
/// </summary>
public sealed class DashboardService(
    IEnumerable<IQueueDepthProvider> depthProviders,
    ConsumerHost consumerHost,
    DlqReader dlqReader,
    DashboardOptions options,
    IDashboardAuditLog audit)
{
    /// <summary>Обзор: суммарные глубины, число консьюмеров, число DLQ-сообщений.</summary>
    public DashboardOverview GetOverview()
    {
        var queues = new List<DashboardQueue>();
        var totalPending = 0;
        var dlqCount = 0;
        var runnerCounts = consumerHost.Runners.GroupBy(r => r.Name).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

        foreach (var provider in depthProviders)
        {
            foreach (var (queue, depth) in provider.QueueDepths)
            {
                var isDlq = queue.EndsWith(".error", StringComparison.OrdinalIgnoreCase)
                    || queue.EndsWith(".poison", StringComparison.OrdinalIgnoreCase)
                    || queue.EndsWith(".expired", StringComparison.OrdinalIgnoreCase);
                runnerCounts.TryGetValue(queue, out var consumers);
                totalPending += depth;
                if (isDlq) dlqCount += depth;
                queues.Add(new DashboardQueue(queue, depth, consumers, isDlq));
            }
        }

        queues.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));

        return new DashboardOverview(
            Mode: options.IsProduction ? "production" : "development",
            TotalPending: totalPending,
            ConsumerCount: consumerHost.Runners.Count,
            DlqCount: dlqCount,
            Queues: queues);
    }

    /// <summary>Просмотр DLQ-сообщений указанной очереди (read-only).</summary>
    public async Task<IReadOnlyList<DlqMessage>> BrowseDeadLettersAsync(
        string queue,
        CancellationToken ct = default)
    {
        var dlq = ResolveDlq(queue);
        return await dlqReader.BrowseAsync(dlq, options.MaxDeadLettersPerBrowse, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Массовый реплей DLQ-очереди в исходные очереди. Опасное действие: требует аудита,
    /// в проде по умолчанию запрещено (идея 482).
    /// </summary>
    public async Task<int> ReplayDeadLettersAsync(
        string queue,
        string user,
        CancellationToken ct = default)
    {
        EnsureDangerousAllowed("replay", user);
        var dlq = ResolveDlq(queue);
        var replayed = await dlqReader.ReplayAllAsync(dlq, options.ReplayMaxPerSecond, ct).ConfigureAwait(false);
        audit.Write(new DashboardAuditRow(DateTimeOffset.UtcNow, user, "replay", queue, $"replayed={replayed}"));
        return replayed;
    }

    /// <summary>
    /// Удаление одного DLQ-сообщения. Опасное действие: требует аудита, в проде по умолчанию запрещено.
    /// </summary>
    public async Task<bool> DeleteDeadLetterAsync(
        string queue,
        Guid messageId,
        string user,
        CancellationToken ct = default)
    {
        EnsureDangerousAllowed("delete", user);
        var dlq = ResolveDlq(queue);
        var deleted = await dlqReader.DeleteAsync(dlq, messageId, ct).ConfigureAwait(false);
        audit.Write(new DashboardAuditRow(DateTimeOffset.UtcNow, user, "delete", queue, $"id={messageId} deleted={deleted}"));
        return deleted;
    }

    private static TransportDestination ResolveDlq(string queue)
        => IsDlqName(queue) ? TransportDestination.Queue(queue) : TransportDestination.Queue(queue + ".error");

    private static bool IsDlqName(string queue)
        => queue.EndsWith(".error", StringComparison.OrdinalIgnoreCase)
            || queue.EndsWith(".poison", StringComparison.OrdinalIgnoreCase)
            || queue.EndsWith(".expired", StringComparison.OrdinalIgnoreCase);

    private void EnsureDangerousAllowed(string action, string user)
    {
        if (options.IsProduction && !options.AllowDangerousOperationsInProduction)
            throw new DashboardAccessDeniedException(
                $"Dangerous action '{action}' is disabled in production (idea 482). " +
                $"User '{user}' is not allowed. Set AllowDangerousOperationsInProduction explicitly.");
    }
}
