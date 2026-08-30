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
    public static IEndpointRouteBuilder MapAvtoBusDashboard(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.ServiceProvider.GetRequiredService<DashboardOptions>();

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
}
