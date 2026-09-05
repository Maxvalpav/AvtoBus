using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Security;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Fail-fast и warnings Production-окружения (аудит, 03 §3.2).
/// Проверяем перегрузки с явным флагом — переменные окружения процесса в тестах не трогаем.
/// </summary>
public class ProductionGuardsTests
{
    [Theory]
    [InlineData("Production", null, true)]
    [InlineData(null, "Production", true)]
    [InlineData("production", null, true)]
    [InlineData("Development", null, false)]
    [InlineData(null, null, false)]
    [InlineData("Staging", null, false)]
    public void Detects_production_environment(string? aspnet, string? dotnet, bool expected)
    {
        Assert.Equal(expected, ProductionGuard.IsProductionEnvironment(aspnet, dotnet));
        Assert.Equal(expected, ProductionSecurityGuard.IsProductionEnvironment(aspnet, dotnet));
    }

    [Fact]
    public void Weak_secret_throws_in_production_only()
    {
        var weak = new SecurityOptions { MasterSecret = "short", RequireSignature = true };
        Assert.Throws<InvalidOperationException>(() =>
            ProductionSecurityGuard.ThrowIfWeakForProduction(weak, isProduction: true));
        // Вне Production — no-op, исключение не бросаем.
        ProductionSecurityGuard.ThrowIfWeakForProduction(weak, isProduction: false);
    }

    [Fact]
    public void Placeholder_secret_throws_in_production()
    {
        var placeholder = new SecurityOptions
        {
            MasterSecret = "shared-secret-0123456789abcdef0123456789",
            RequireSignature = true,
        };
        // Длинный, но словарный — ловим только точные плейсхолдеры, этот проходит длину.
        ProductionSecurityGuard.ThrowIfWeakForProduction(placeholder, isProduction: true);

        var exact = new SecurityOptions
        {
            MasterSecret = "shared-secret",
            RequireSignature = true,
        };
        Assert.Throws<InvalidOperationException>(() =>
            ProductionSecurityGuard.ThrowIfWeakForProduction(exact, isProduction: true));
    }

    [Fact]
    public void Disabled_signature_throws_in_production()
    {
        var noSig = new SecurityOptions
        {
            MasterSecret = "long-enough-secret-for-production-use-0123456789",
            RequireSignature = false,
        };
        Assert.Throws<InvalidOperationException>(() =>
            ProductionSecurityGuard.ThrowIfWeakForProduction(noSig, isProduction: true));
        ProductionSecurityGuard.ThrowIfWeakForProduction(noSig, isProduction: false);
    }

    [Fact]
    public void Strong_secret_passes_in_production()
    {
        var strong = new SecurityOptions
        {
            MasterSecret = "long-enough-secret-for-production-use-0123456789",
            RequireSignature = true,
        };
        ProductionSecurityGuard.ThrowIfWeakForProduction(strong, isProduction: true);
    }

    [Fact]
    public void InMemory_only_warns_in_production()
    {
        var options = new BusOptions();
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new InMemoryTransport());

        ProductionGuard.CheckTransports(options, services, isProduction: true);
        Assert.Single(options.StartupWarnings);

        var dev = new BusOptions();
        ProductionGuard.CheckTransports(dev, services, isProduction: false);
        Assert.Empty(dev.StartupWarnings);
    }

    [Fact]
    public void External_transport_no_warning()
    {
        var options = new BusOptions();
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(_ => new InMemoryTransport());

        // Фабричная регистрация без имени типа — считаем внешним транспортом.
        ProductionGuard.CheckTransports(options, services, isProduction: true);
        Assert.Empty(options.StartupWarnings);
    }

    [Fact]
    public void Experimental_warning_is_added()
    {
        var options = new BusOptions();
        ProductionGuard.WarnExperimental(options, "AvtoBus.Bridge");
        Assert.Single(options.StartupWarnings);
        Assert.Contains("AvtoBus.Bridge", options.StartupWarnings[0]);
    }
}
