using AvtoBus.Configuration;
using AvtoBus.Observability;
using AvtoBus.Runtime;
using Microsoft.AspNetCore.Http;

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
        // Snapshot runners to avoid concurrent modification
        var runnersSnapshot = consumerHost.Runners.ToArray();
        var runnerCounts = runnersSnapshot.GroupBy(r => r.Name).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

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
            ConsumerCount: runnersSnapshot.Length,
            DlqCount: dlqCount,
            Queues: queues);
    }

    /// <summary>
    /// Просмотр DLQ-сообщений указанной очереди (read-only). Копия для отображения:
    /// при <see cref="DashboardOptions.SanitizeBrowse"/> redact заголовков, маскирование
    /// PII в JSON-телах и обрезка тел; фильтр по <see cref="DashboardOptions.TenantId"/>.
    /// </summary>
    public async Task<IReadOnlyList<DlqMessage>> BrowseDeadLettersAsync(
        string queue,
        CancellationToken ct = default)
    {
        var dlq = ResolveDlq(queue);
        var messages = await dlqReader.BrowseAsync(dlq, options.MaxDeadLettersPerBrowse, ct).ConfigureAwait(false);

        if (options.TenantId is not null)
            messages = messages.Where(m => m.Envelope.TenantId == options.TenantId).ToArray();

        if (!options.SanitizeBrowse)
            return messages;

        return messages.Select(Sanitize).ToArray();
    }

    private DlqMessage Sanitize(DlqMessage message)
    {
        var envelope = message.Envelope;

        Dictionary<string, string>? headers = null;
        foreach (var kv in envelope.Headers)
        {
            var value = options.RedactedHeaders.Contains(kv.Key) ? "***redacted***" : kv.Value;
            (headers ??= new Dictionary<string, string>(StringComparer.Ordinal))[kv.Key] = value;
        }

        var body = envelope.Body;
        if (body.Length > options.MaxBodyPreviewBytes)
            body = body[..options.MaxBodyPreviewBytes];
        body = MaskPiiFields(body);

        return message with
        {
            Envelope = envelope with
            {
                Headers = headers is null
                    ? envelope.Headers
                    : System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(headers, StringComparer.Ordinal),
                Body = body.ToArray(),
            },
        };
    }

    /// <summary>
    /// Best-effort маскирование PII в JSON-теле для глаз: поля с high-signal именами
    /// (email/phone/password/token/card/...) заменяются детерминированной маской.
    /// Полная защита — [PersonalData] + PiiMaskingEnabled на consume-пути; здесь только
    /// вторая линия для дашборда, не знающего CLR-типы контрактов.
    /// </summary>
    public static ReadOnlyMemory<byte> MaskPiiFields(ReadOnlyMemory<byte> body)
    {
        string text;
        try { text = System.Text.Encoding.UTF8.GetString(body.Span); }
        catch { return body; }

        System.Text.Json.JsonDocument doc;
        try { doc = System.Text.Json.JsonDocument.Parse(text); }
        catch (System.Text.Json.JsonException) { return body; }
        using (doc)
        {
            if (doc.RootElement.ValueKind is not System.Text.Json.JsonValueKind.Object)
                return body;
            var masked = MaskElement(doc.RootElement);
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(masked);
        }
    }

    private static readonly string[] PiiNameHints =
    [
        "email", "e-mail", "mail", "phone", "tel", "mobile", "fax",
        "password", "passwd", "pwd", "secret", "token", "apikey", "api_key",
        "card", "cvv", "cvc", "ssn", "passport", "inn", "snils", "account",
    ];

    private static object? MaskElement(System.Text.Json.JsonElement element) => element.ValueKind switch
    {
        System.Text.Json.JsonValueKind.Object => element.EnumerateObject().ToDictionary(
            p => p.Name,
            p => IsPiiName(p.Name) && p.Value.ValueKind is System.Text.Json.JsonValueKind.String
                ? (object?)AvtoBus.Diagnostics.PiiMasker.Mask(p.Value.GetString())
                : MaskElement(p.Value),
            StringComparer.Ordinal),
        System.Text.Json.JsonValueKind.Array => element.EnumerateArray().Select(MaskElement).ToArray(),
        System.Text.Json.JsonValueKind.String => element.GetString(),
        System.Text.Json.JsonValueKind.Number => element.GetDecimal(),
        System.Text.Json.JsonValueKind.True => true,
        System.Text.Json.JsonValueKind.False => false,
        System.Text.Json.JsonValueKind.Null => null,
        _ => element.GetRawText(),
    };

    private static bool IsPiiName(string name)
    {
        foreach (var hint in PiiNameHints)
            if (name.Contains(hint, StringComparison.OrdinalIgnoreCase))
                return true;
        return false;
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

    public static bool TryHandleAccessDenied(Exception ex, out Microsoft.AspNetCore.Http.IResult? result)
    {
        if (ex is DashboardAccessDeniedException denied)
        {
            result = Results.Problem(detail: denied.Message, statusCode: StatusCodes.Status403Forbidden, title: "Forbidden");
            return true;
        }
        result = null;
        return false;
    }
}
