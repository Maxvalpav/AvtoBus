using System.Collections.Immutable;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AvtoBus.AnalyzerTests;

/// <summary>Собранные диагностики от запуска набора анализаторов над фрагментом кода.</summary>
internal sealed class AnalyzerResult
{
    public required ImmutableArray<Diagnostic> Diagnostics { get; init; }

    public IEnumerable<Diagnostic> OfId(string id) => Diagnostics.Where(d => d.Id == id);

    public bool Has(string id) => OfId(id).Any();
}

/// <summary>
/// Запускает набор Roslyn-анализаторов над фрагментом исходника. Ссылается на реальную сборку
/// Core, чтобы символы ICommand/IEvent/IBus резолвились (AVB004/005 зависят от типов контрактов).
/// </summary>
internal static class AnalyzerDriver
{
    private static readonly Assembly[] Seed =
    [
        typeof(AvtoBus.IBus).Assembly,
        typeof(System.Text.Json.JsonSerializer).Assembly,
    ];

    public static AnalyzerResult Run(string source, params DiagnosticAnalyzer[] analyzers)
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

        var tree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest), path: "source.cs");

        var compilation = CSharpCompilation.Create(
            "AvtoBus.AnalyzerTestAssembly",
            [tree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var withAnalyzers = compilation.WithAnalyzers(analyzers.ToImmutableArray());
        var diagnostics = withAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return new AnalyzerResult { Diagnostics = diagnostics };
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
