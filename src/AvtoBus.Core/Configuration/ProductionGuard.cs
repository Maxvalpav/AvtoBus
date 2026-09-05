using AvtoBus;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Configuration;

/// <summary>
/// Проверки окружения Production (аудит, недели 2–3): fail-fast на слабые настройки
/// и предупреждения в лог. Production считается ЯВНО выставленным окружением
/// (<c>ASPNETCORE_ENVIRONMENT=Production</c> или <c>DOTNET_ENVIRONMENT=Production</c>) —
/// отсутствие переменной ничего не проверяет, чтобы не ломать тесты и dev-запуски.
/// </summary>
public static class ProductionGuard
{
    /// <summary>Окружение явно выставлено в Production.</summary>
    public static bool IsProduction =>
        IsProductionEnvironment(
            Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"),
            Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT"));

    /// <summary>Чистая проверка значений окружения (для тестов).</summary>
    public static bool IsProductionEnvironment(string? aspNetCoreEnvironment, string? dotNetEnvironment)
        => IsProdValue(aspNetCoreEnvironment) || IsProdValue(dotNetEnvironment);

    private static bool IsProdValue(string? value)
        => string.Equals(value, "Production", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Предупреждение об experimental-пакете (уровни зрелости, 03 §1.1):
    /// без гарантий совместимости, API может меняться в миноре.
    /// </summary>
    public static void WarnExperimental(BusOptions options, string packageId)
        => options.AddStartupWarning(
            $"Пакет {packageId} — experimental: без гарантий совместимости, API может меняться в минорных версиях.");

    /// <summary>
    /// InMemory как единственный транспорт в Production — почти наверняка ошибка
    /// конфигурации (сообщения не переживают рестарт). Только предупреждение:
    /// бывают валидные случаи (воркер-потребитель с внешним брокером на чтение? нет —
    /// тогда транспорт был бы внешним; но ломать старт не будем).
    /// </summary>
    public static void CheckTransports(BusOptions options, IServiceCollection services)
        => CheckTransports(options, services, IsProduction);

    /// <summary>Перегрузка с явным флагом окружения (для тестов).</summary>
    internal static void CheckTransports(BusOptions options, IServiceCollection services, bool isProduction)
    {
        if (!isProduction)
            return;
        var implNames = services
            .Where(d => d.ServiceType == typeof(ITransport))
            .Select(d => d.ImplementationType?.Name ?? d.ImplementationInstance?.GetType().Name ?? "?")
            .ToList();
        if (implNames.Count == 0 || implNames.All(n =>
                n.Contains("InMemory", StringComparison.OrdinalIgnoreCase) ||
                n.Contains("Local", StringComparison.OrdinalIgnoreCase)))
        {
            options.AddStartupWarning(
                "AvtoBus в Production работает только на in-memory транспорте: " +
                "сообщения теряются при рестарте. Подключите персистентный транспорт (RabbitMQ/Kafka/...) " +
                "или выставьте окружение явно не-Production, если это намеренно.");
        }
    }
}
