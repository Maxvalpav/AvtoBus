using AvtoBus.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;

namespace AvtoBus.Tests;

/// <summary>
/// Провайдеры секретов (аудит 03 §3.1): мастер-секрет резолвится из конфигурации,
/// env, файла или цепочки — а не лежит строкой в коде.
/// </summary>
public class SecretProviderTests
{
    private sealed class DictionaryConfiguration(Dictionary<string, string?> values) : IConfiguration
    {
        public string? this[string key]
        {
            get => values.TryGetValue(key, out var v) ? v : null;
            set => values[key] = value;
        }

        public IEnumerable<IConfigurationSection> GetChildren() => [];
        public IChangeToken GetReloadToken() => new CancellationChangeToken(CancellationToken.None);
        public IConfigurationSection GetSection(string key) => new Section(values.TryGetValue(key, out var v) ? v : null, key);
        public void Dispose() { }

        private sealed class Section(string? value, string key) : IConfigurationSection
        {
            public string? this[string k] { get => null; set { } }
            public string Key => key;
            public string Path => key;
            public string? Value { get => value; set { } }
            public IEnumerable<IConfigurationSection> GetChildren() => [];
            public IChangeToken GetReloadToken() => new CancellationChangeToken(CancellationToken.None);
            public IConfigurationSection GetSection(string k) => this;
        }
    }

    private sealed class StubProvider(Func<string, string?> resolve) : ISecretProvider
    {
        public string? GetSecret(string name) => resolve(name);
    }

    [Fact]
    public void Configuration_provider_reads_key()
    {
        var provider = new ConfigurationSecretProvider(
            new DictionaryConfiguration(new() { ["AvtoBus:MasterSecret"] = "config-secret-value" }));
        Assert.Equal("config-secret-value", provider.GetSecret("AvtoBus:MasterSecret"));
        Assert.Null(provider.GetSecret("Missing"));
    }

    [Fact]
    public void Environment_provider_maps_name_with_prefix()
    {
        Environment.SetEnvironmentVariable("AVTOBUS_TEST_SECRET_XYZ", "env-secret-value");
        try
        {
            var provider = new EnvironmentSecretProvider("AVTOBUS_");
            Assert.Equal("env-secret-value", provider.GetSecret("TEST_SECRET_XYZ"));
            Assert.Null(provider.GetSecret("TEST_SECRET_DEFINITELY_MISSING"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("AVTOBUS_TEST_SECRET_XYZ", null);
        }
    }

    [Fact]
    public void File_provider_reads_secret_and_trims_newline()
    {
        var dir = Directory.CreateTempSubdirectory("avtobus-secrets").FullName;
        try
        {
            File.WriteAllText(Path.Combine(dir, "master"), "file-secret-value\n");
            var provider = new FileSecretProvider(dir);
            Assert.Equal("file-secret-value", provider.GetSecret("master"));
            Assert.Null(provider.GetSecret("nope"));
            Assert.Null(provider.GetSecret("../escape"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Composite_returns_first_non_empty()
    {
        var provider = new CompositeSecretProvider(
            new StubProvider(_ => null),
            new StubProvider(_ => ""),
            new StubProvider(_ => "winner"),
            new StubProvider(_ => "loser"));
        Assert.Equal("winner", provider.GetSecret("any"));
        Assert.Null(new CompositeSecretProvider(new StubProvider(_ => null)).GetSecret("any"));
    }

    [Fact]
    public void Options_resolve_fills_empty_master_secret()
    {
        var options = new SecurityOptions();
        options.UseSecretProvider(new StubProvider(_ => "resolved-secret"));
        options.ResolveMasterSecret();
        Assert.Equal("resolved-secret", options.MasterSecret);
    }

    [Fact]
    public void Options_resolve_prefers_explicit_string()
    {
        var options = new SecurityOptions { MasterSecret = "explicit" };
        options.UseSecretProvider(new StubProvider(_ => "from-provider"));
        options.ResolveMasterSecret();
        Assert.Equal("explicit", options.MasterSecret);
    }

    [Fact]
    public void Provider_placeholder_still_fails_production_guard()
    {
        var options = new SecurityOptions { RequireSignature = true };
        options.UseSecretProvider(new StubProvider(_ => "shared-secret"));
        options.ResolveMasterSecret();
        Assert.Throws<InvalidOperationException>(() =>
            ProductionSecurityGuard.ThrowIfWeakForProduction(options, isProduction: true));
    }

    [Fact]
    public void Provider_strong_secret_passes_production_guard()
    {
        var options = new SecurityOptions { RequireSignature = true };
        options.UseSecretProvider(new StubProvider(_ => "this-is-a-strong-production-secret-32+"));
        options.ResolveMasterSecret();
        ProductionSecurityGuard.ThrowIfWeakForProduction(options, isProduction: true);
    }
}
