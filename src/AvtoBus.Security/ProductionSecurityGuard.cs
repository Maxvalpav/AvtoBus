namespace AvtoBus.Security;

/// <summary>
/// Fail-fast слабых security-настроек в Production (аудит, 03 §3.2): короткие
/// и словарные секреты, выключенная проверка подписи. Срабатывает ТОЛЬКО при явно
/// выставленном Production-окружении (<c>ASPNETCORE_ENVIRONMENT=Production</c> или
/// <c>DOTNET_ENVIRONMENT=Production</c>) — тесты и dev-запуски не затрагиваются.
/// </summary>
public static class ProductionSecurityGuard
{
    /// <summary>Минимальная длина мастер-секрета (символов) для Production.</summary>
    public const int MinProductionSecretLength = 32;

    private static readonly string[] KnownPlaceholders =
    [
        "shared-secret", "changeme", "password", "secret", "test", "123456",
        "avtobus-development-only",
    ];

    /// <summary>Чистая проверка окружения (для тестов): явный Production или нет.</summary>
    public static bool IsProductionEnvironment(string? aspNetCoreEnvironment, string? dotNetEnvironment)
        => IsProdValue(aspNetCoreEnvironment) || IsProdValue(dotNetEnvironment);

    private static bool IsProdValue(string? value)
        => string.Equals(value, "Production", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Бросает <see cref="InvalidOperationException"/>, если окружение — Production,
    /// а настройки слабые. Вне Production — no-op.
    /// </summary>
    public static void ThrowIfWeakForProduction(SecurityOptions options)
        => ThrowIfWeakForProduction(options, IsProductionEnvironment(
            Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"),
            Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")));

    /// <summary>Перегрузка с явным флагом окружения (для тестов).</summary>
    public static void ThrowIfWeakForProduction(SecurityOptions options, bool isProduction)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!isProduction)
            return;

        if (!options.RequireSignature)
            throw new InvalidOperationException(
                "SecurityOptions: RequireSignature=false запрещён в Production. " +
                "Непроверенные подписи — это подделка сообщений от любого, кто знает транспорт. " +
                "Включите RequireSignature или выставьте окружение явно не-Production, если это намеренно.");

        // Явные ключи (UseKeys/UseGeneratedKeys) — криптографически сильные по построению.
        if (options.Keys.SigningKey.Length != 0)
            return;

        var secret = options.MasterSecret;
        if (secret.Length < MinProductionSecretLength)
            throw new InvalidOperationException(
                $"SecurityOptions: MasterSecret короче {MinProductionSecretLength} символов " +
                $"({secret.Length}) — недостаточен для Production. Возьмите секрет из Key Vault / K8s secrets.");
        if (KnownPlaceholders.Contains(secret, StringComparer.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "SecurityOptions: MasterSecret равен известному плейсхолдеру — замените на настоящий секрет из конфигурации.");
    }
}
