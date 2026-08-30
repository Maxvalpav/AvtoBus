using System.Collections.Immutable;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using AvtoBus.Generators;

namespace AvtoBus.GeneratorTests;

/// <summary>
/// Запускает <see cref="HandlerGenerator"/> против фрагмента исходника и возвращает
/// сгенерированные файлы и диагностики. Ссылается на реальные сборки Core и BCL,
/// чтобы символы IConsumer&lt;T&gt;, ICommand, IEvent резолвились.
/// </summary>
internal sealed class GeneratorResult
{
    public required ImmutableDictionary<string, string> Sources { get; init; }

    public required ImmutableArray<Diagnostic> Diagnostics { get; init; }

    public bool HasSource(string name) => Sources.ContainsKey(name);

    public string Source(string name) => Sources[name];

    public IEnumerable<Diagnostic> Errors => Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error);

    public IEnumerable<Diagnostic> Warnings => Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Warning);
}

internal static class GeneratorDriver
{
    /// <summary>Сборки, которые должны быть видны компиляции: Core и все его зависимости.</summary>
    private static readonly Assembly[] Seed =
    [
        typeof(AvtoBus.IBus).Assembly,
        typeof(Microsoft.Extensions.DependencyInjection.IServiceCollection).Assembly,
        typeof(System.Text.Json.JsonSerializer).Assembly,
    ];

    public static GeneratorResult Run(string source, string assemblyName = "AvtoBus.TestAssembly")
        => Run([source], assemblyName);

    public static GeneratorResult Run(IReadOnlyList<string> sources, string assemblyName = "AvtoBus.TestAssembly")
    {
        var references = new List<MetadataReference>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<Assembly>(Seed);

        while (pending.Count > 0)
        {
            var assembly = pending.Dequeue();
            var name = assembly.GetName().Name!;
            if (!seen.Add(name))
                continue;

            references.Add(MetadataReference.CreateFromFile(assembly.Location));

            foreach (var reference in assembly.GetReferencedAssemblies())
            {
                var loaded = AppDomain.CurrentDomain.GetAssemblies()
                                  .FirstOrDefault(a => a.GetName().Name == reference.Name)
                              ?? TryLoadFromTpa(reference.Name);

                if (loaded is not null)
                    pending.Enqueue(loaded);
            }
        }

        var syntaxTrees = sources
            .Select((s, i) => CSharpSyntaxTree.ParseText(s, new CSharpParseOptions(LanguageVersion.Latest), path: $"source{i}.cs"))
            .ToArray();

        var compilation = CSharpCompilation.Create(
            assemblyName,
            syntaxTrees,
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        var generator = new HandlerGenerator().AsSourceGenerator();
        var driver = CSharpGeneratorDriver.Create([generator]).RunGeneratorsAndUpdateCompilation(
            compilation,
            out var outputCompilation,
            out var diagnostics);

        var runResult = driver.GetRunResult();
        var generated = runResult.Results[0].GeneratedSources;

        var generatedSources = generated.ToImmutableDictionary(g => g.HintName, g => g.SourceText.ToString());

        return new GeneratorResult
        {
            Sources = generatedSources,
            Diagnostics = diagnostics.AddRange(runResult.Diagnostics).ToImmutableArray(),
        };
    }

    private static Assembly? TryLoadFromTpa(string? name)
    {
        if (string.IsNullOrEmpty(name))
            return null;

        var tpa = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES");
        if (string.IsNullOrEmpty(tpa))
            return null;

        var match = tpa.Split(Path.PathSeparator)
            .Select(Path.GetFileNameWithoutExtension)
            .FirstOrDefault(f => f == name);

        return match is null ? null : Assembly.Load(name);
    }
}
