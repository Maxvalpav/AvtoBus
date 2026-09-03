using AvtoBus.Configuration;
using AvtoBus.Outbox.EfCore;
using AvtoBus.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>Прод-пресет: один вызов вместо пяти, значения проверяемы.</summary>
public class ProductionDefaultsTests
{
    [Fact]
    public async Task Core_preset_applies_reliability_defaults()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.UseProductionDefaults());

        var options = harness.Services.GetRequiredService<BusOptions>();

        Assert.Equal(3, options.Recoverability.ImmediateRetryCount);
        Assert.Equal(5, options.Recoverability.DelayedRetryCount);
        Assert.Equal(TimeSpan.FromHours(24), options.InboxWindow);
        Assert.Equal(5, options.CircuitBreakerThreshold);
        Assert.Equal(TimeSpan.FromSeconds(30), options.CircuitBreakerDuration);
    }

    [Fact]
    public void Full_preset_wires_outbox_and_signing()
    {
        using var sp = BuildServices(bus => bus.UseProductionDefaults<TestOutboxContext>(o =>
        {
            o.MasterSecret = "preset-secret";
            o.PiiMaskSalt = "preset-salt";
        }));

        var options = sp.GetRequiredService<BusOptions>();
        Assert.Equal(3, options.Recoverability.ImmediateRetryCount);
        Assert.Equal("preset-salt", options.PiiMaskSalt);

        var security = sp.GetRequiredService<IEnvelopeSecurity>();
        Assert.True(security.IsEnabled);

        using var scope = sp.CreateScope();
        Assert.NotNull(scope.ServiceProvider.GetRequiredService<IOutbox>());
    }

    [Fact]
    public void Full_preset_without_secret_skips_signing()
    {
        using var sp = BuildServices(bus => bus.UseProductionDefaults<TestOutboxContext>());

        // Без мастер-секрета fail-closed не включается молча: безопасности нет,
        // доставка надёжна.
        Assert.Null(sp.GetService<IEnvelopeSecurity>());
        var options = sp.GetRequiredService<BusOptions>();
        Assert.Equal(5, options.Recoverability.DelayedRetryCount);
    }

    [Fact]
    public void Base_preset_with_secret_fails_fast()
    {
        // Аудит B5: молчаливое игнорирование MasterSecret базовым пресетом запрещено.
        Assert.Throws<InvalidOperationException>(() =>
            BuildServices(bus => bus.UseProductionDefaults(o => o.MasterSecret = "preset-secret")));
        Assert.Throws<InvalidOperationException>(() =>
            BuildServices(bus => bus.UseProductionDefaults(o => o.OutboundRatePerSecond = 100)));
    }

    private static ServiceProvider BuildServices(Action<BusConfigurator> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // ПровайдерDummy: соединение не открывается — проверяется только регистрация.
        services.AddDbContext<TestOutboxContext>(o => o.UseNpgsql("Host=localhost;Database=dummy"));
        services.AddAvtoBus(configure);
        return services.BuildServiceProvider();
    }
}
