namespace AvtoBus.Dashboard;

/// <summary>
/// Настройки встраиваемого дашборда (док 23, идея 482).
/// Опасные операции (replay, delete, pause) в проде по умолчанию отключены:
/// включение требует явного согласия оператора.
/// </summary>
public sealed class DashboardOptions
{
    /// <summary>Базовый путь группы endpoints, по умолчанию <c>/bus</c>.</summary>
    public string RoutePrefix { get; set; } = "/bus";

    /// <summary>Имя authorization policy, которой защищены все endpoint-ы дашборда.</summary>
    public string PolicyName { get; set; } = "AvtoBusDashboard";

    /// <summary>
    /// Среда продакшена: если <c>true</c>, опасные действия (replay/delete) выбросят
    /// <see cref="DashboardAccessDeniedException"/>, пока не выставлен
    /// <see cref="AllowDangerousOperationsInProduction"/>.
    /// </summary>
    public bool IsProduction { get; set; }

    /// <summary>Явное разрешение опасных действий в проде (идея 482). По умолчанию выключено.</summary>
    public bool AllowDangerousOperationsInProduction { get; set; }

    /// <summary>Потолок live-tail трафика на консьюмера, байт/с.</summary>
    public int MaxLiveTailBytesPerSecond { get; set; } = 2 * 1024 * 1024;

    /// <summary>Максимум сообщений DLQ за один просмотр.</summary>
    public int MaxDeadLettersPerBrowse { get; set; } = 100;

    /// <summary>
    /// Санитизировать просмотр DLQ (дефолт true): redact чувствительных заголовков,
    /// best-effort маскирование PII-полей в JSON-телах, обрезка гигантских тел.
    /// Browse — только для глаз (реплей перечитывает очередь заново), поэтому
    /// копия для отображения может отличаться от оригинала.
    /// </summary>
    public bool SanitizeBrowse { get; set; } = true;

    /// <summary>
    /// Фильтр тенанта для просмотра DLQ (уровень B/C): null — все тенанты.
    /// Оператор видит только сообщения своего тенанта.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>Потолок тела сообщения в просмотре DLQ, байт. Большее обрезается.</summary>
    public int MaxBodyPreviewBytes { get; set; } = 32 * 1024;

    /// <summary>Заголовки, всегда redact'ящиеся в просмотре (секреты/стеки/PII).</summary>
    public HashSet<string> RedactedHeaders { get; } =
    [
        "avtobus-user",
        "avtobus-exception-stack",
    ];

    /// <summary>Максимум сообщений DLQ в секунду при массовом реплее (идея 168).</summary>
    public int ReplayMaxPerSecond { get; set; } = 10;
}

/// <summary>Опасное действие дашборда запрещено политикой (идея 482).</summary>
public sealed class DashboardAccessDeniedException(string reason) : Exception(reason);
