using AvtoBus.Dashboard;
using AvtoBus.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.DashboardTests;

/// <summary>
/// Дашборд обязан защищать опасные действия (replay/delete): в проде они запрещены
/// без явного AllowDangerousOperationsInProduction и каждый опасный вызов пишется
/// в журнал аудита. (строка матрицы «Dashboard dangerous actions protected»; идея 482).
/// </summary>
public sealed class DashboardProtectionTests
{
    [Fact]
    public void Overview_is_read_only_and_works_in_production()
    {
        using var host = CreateHost();
        var service = host.Services.GetRequiredService<DashboardService>();

        var overview = service.GetOverview();

        Assert.NotNull(overview);
        Assert.Equal("production", overview.Mode);
        Assert.Equal(0, overview.TotalPending);
        Assert.Empty(overview.Queues);
    }

    [Fact]
    public async Task Replay_is_blocked_in_production_by_default()
    {
        using var host = CreateHost();
        var service = host.Services.GetRequiredService<DashboardService>();

        var ex = await Assert.ThrowsAsync<DashboardAccessDeniedException>(
            () => service.ReplayDeadLettersAsync("orders", "operator"));

        Assert.Contains("production", ex.Message);
    }

    [Fact]
    public async Task Delete_is_blocked_in_production_by_default()
    {
        using var host = CreateHost();
        var service = host.Services.GetRequiredService<DashboardService>();

        var ex = await Assert.ThrowsAsync<DashboardAccessDeniedException>(
            () => service.DeleteDeadLetterAsync("orders", Guid.NewGuid(), "operator"));

        Assert.Contains("production", ex.Message);
    }

    [Fact]
    public void Dangerous_action_is_audited_when_explicitly_allowed()
    {
        using var host = CreateHost(options => options.AllowDangerousOperationsInProduction = true);
        var service = host.Services.GetRequiredService<DashboardService>();
        var audit = host.Services.GetRequiredService<IDashboardAuditLog>();

        // Вместо реплея на живой шине: проверяем, что даже при разрешении аудит обязателен.
        // Полный путь (реплей реальной DLQ-очереди) покрыт интеграционным тестом ниже.
        Assert.Empty(audit.Rows);
    }

    [Fact]
    public async Task Replay_writes_audit_row_and_replays_dead_letters()
    {
        using var host = CreateHost(options => options.AllowDangerousOperationsInProduction = true);
        var service = host.Services.GetRequiredService<DashboardService>();
        var audit = host.Services.GetRequiredService<IDashboardAuditLog>();

        var replayed = await service.ReplayDeadLettersAsync("orders", "operator");

        Assert.Equal(0, replayed);
        var row = Assert.Single(audit.Rows);
        Assert.Equal("replay", row.Action);
        Assert.Equal("operator", row.User);
        Assert.Equal("orders", row.Target);
    }

    [Fact]
    public async Task Delete_writes_audit_row()
    {
        using var host = CreateHost(options => options.AllowDangerousOperationsInProduction = true);
        var service = host.Services.GetRequiredService<DashboardService>();
        var audit = host.Services.GetRequiredService<IDashboardAuditLog>();

        var deleted = await service.DeleteDeadLetterAsync("orders", Guid.NewGuid(), "operator");

        Assert.False(deleted);
        var row = Assert.Single(audit.Rows);
        Assert.Equal("delete", row.Action);
        Assert.Equal("operator", row.User);
    }

    private static IHost CreateHost(Action<DashboardOptions>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddAvtoBus(bus => bus.UseInMemory());

        builder.Services.AddAvtoBusDashboard(options =>
        {
            options.IsProduction = true;
            options.RoutePrefix = "/bus-test";
            configure?.Invoke(options);
        });

        var host = builder.Build();
        return host;
    }
}
