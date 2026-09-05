using System.Reflection;

namespace AvtoBus.Tests;

/// <summary>
/// Архитектурный тест тонкого ядра (E-01): Hangfire/Actors/Canvas/Mongo живут в
/// своих пакетах, а граф зависимостей AvtoBus.Core — только Microsoft.*/System.*
/// (+ OpenTelemetry). Без внешних тестовых фреймворков — чистая рефлексия.
/// </summary>
public class CoreBoundariesTests
{
    private static readonly string[] ExtractedNamespaces =
    [
        "AvtoBus.Hangfire", "AvtoBus.Mongo", "AvtoBus.Actors", "AvtoBus.Canvas",
    ];

    [Fact]
    public void Core_contains_no_extracted_namespaces()
    {
        var core = typeof(IBus).Assembly;
        var leaked = core.GetTypes()
            .Select(t => t.Namespace ?? "")
            .Where(ns => ExtractedNamespaces.Any(e =>
                ns == e || ns.StartsWith(e + ".", StringComparison.Ordinal)))
            .Distinct()
            .OrderBy(ns => ns)
            .ToList();
        Assert.True(leaked.Count == 0,
            $"AvtoBus.Core содержит типы вынесенных областей: {string.Join(", ", leaked)}");
    }

    [Fact]
    public void Extracted_types_live_in_own_assemblies()
    {
        var core = typeof(IBus).Assembly;
        Assert.NotSame(core, typeof(AvtoBus.Hangfire.HangfireJobEnvelope).Assembly);
        Assert.NotSame(core, typeof(AvtoBus.Mongo.MongoOutboxOptions).Assembly);
        Assert.NotSame(core, typeof(AvtoBus.Actors.IActor).Assembly);
        Assert.NotSame(core, typeof(AvtoBus.Canvas.Canvas).Assembly);
    }

    [Fact]
    public void Core_references_only_platform_assemblies()
    {
        var core = typeof(IBus).Assembly;
        var bad = core.GetReferencedAssemblies()
            .Select(a => a.Name ?? "")
            .Where(name => !IsPlatformAssembly(name))
            .OrderBy(name => name)
            .ToList();
        Assert.True(bad.Count == 0,
            $"AvtoBus.Core ссылается на неплатформенные сборки: {string.Join(", ", bad)}");
    }

    private static bool IsPlatformAssembly(string name)
        => name.StartsWith("Microsoft.", StringComparison.Ordinal)
            || name.StartsWith("System", StringComparison.Ordinal)
            || name is "mscorlib" or "netstandard"
            || name.StartsWith("OpenTelemetry", StringComparison.Ordinal);
}
