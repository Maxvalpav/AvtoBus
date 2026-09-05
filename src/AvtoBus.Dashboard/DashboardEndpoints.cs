using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Dashboard;

/// <summary>
/// Встраиваемые endpoint-ы дашборда (док 23): read-only обзор/очереди/DLQ и
/// опасные действия (replay/delete), защищённые policy <see cref="DashboardOptions.PolicyName"/>.
/// </summary>
public static class DashboardEndpoints
{
    /// <summary>
    /// Маппит группу <c>GET/DELETE/POST</c> дашборда под базовым путём
    /// <see cref="DashboardOptions.RoutePrefix"/> с <c>RequireAuthorization(PolicyName)</c>.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Minimal-API лямбды дашборда оперируют только внутренними DTO; RDG приложения покрывает их при trimming.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Minimal-API лямбды дашборда оперируют только внутренними DTO; RDG приложения покрывает их при NativeAOT.")]
    public static IEndpointRouteBuilder MapAvtoBusDashboard(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.ServiceProvider.GetRequiredService<DashboardOptions>();

        // Fail-fast в Production (аудит, 03 §3.2): дашборд без auth-политики запрещён.
        // RequireAuthorization с пустым именем молча ничего не защищает — ловим это при старте.
        if (string.IsNullOrWhiteSpace(options.PolicyName))
            throw new InvalidOperationException(
                "DashboardOptions: PolicyName пуст — дашборд остался бы без авторизации. " +
                "Задайте имя authorization policy или не маппите дашборд.");
        if (IsProductionEnvironment() && !options.IsProduction)
        {
            // Авто-харденинг: в Production опасные действия (replay/delete) блокируются,
            // пока оператор явно не разрешит их через AllowDangerousOperationsInProduction.
            options.IsProduction = true;
        }

        // Пакет experimental (уровни зрелости, 03 §1.1): предупреждение в стартовых логах.
        endpoints.ServiceProvider
            .GetService<AvtoBus.Configuration.BusOptions>()?
            .AddStartupWarning("Пакет AvtoBus.Dashboard — experimental: без гарантий совместимости, API может меняться в минорных версиях.");

        var group = endpoints.MapGroup(options.RoutePrefix).RequireAuthorization(options.PolicyName);

        group.MapGet("/api/overview", async (DashboardService service, HttpContext http) =>
        {
            var overview = service.GetOverview();
            return Results.Ok(overview);
        });

        group.MapGet("/api/dlq/{queue}", async (DashboardService service, string queue, CancellationToken ct) =>
        {
            var messages = await service.BrowseDeadLettersAsync(queue, ct).ConfigureAwait(false);
            return Results.Ok(messages);
        });

        group.MapPost("/api/dlq/{queue}/replay", async (
            DashboardService service,
            HttpContext http,
            string queue,
            CancellationToken ct) =>
        {
            var user = http.User.Identity?.Name ?? "anonymous";
            var replayed = await service.ReplayDeadLettersAsync(queue, user, ct).ConfigureAwait(false);
            return Results.Ok(new { replayed });
        });

        group.MapDelete("/api/dlq/{queue}/messages/{messageId:guid}", async (
            DashboardService service,
            HttpContext http,
            string queue,
            Guid messageId,
            CancellationToken ct) =>
        {
            var user = http.User.Identity?.Name ?? "anonymous";
            var deleted = await service.DeleteDeadLetterAsync(queue, messageId, user, ct).ConfigureAwait(false);
            return Results.Ok(new { deleted });
        });

        return endpoints;
    }

    private static bool IsProductionEnvironment()
        => string.Equals(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"), "Production", StringComparison.OrdinalIgnoreCase)
            || string.Equals(Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT"), "Production", StringComparison.OrdinalIgnoreCase);
}
