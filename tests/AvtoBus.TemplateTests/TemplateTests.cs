using System.Diagnostics;
using System.Text;

namespace AvtoBus.TemplateTests;

/// <summary>
/// Smoke-тесты шаблонов (идея 401): пакет упаковывается, ставится в изолированный hive,
/// шаблоны инстанцируются с выбором транспорта, а сгенерированные проекты собираются
/// против локального NuGet-feed собранного из исходников AvtoBus.
/// </summary>
public class TemplateTests
{
    private static readonly string RepoRoot = FindRepoRoot();

    public static TheoryData<string, string, string, string?> Transports => new()
    {
        { "avtobus-worker", "inmemory", "UseInMemory", null },
        { "avtobus-worker", "kafka", "UseKafka", "AvtoBus.Kafka" },
        { "avtobus-worker", "redis", "UseRedis", "AvtoBus.Redis" },
        { "avtobus-webapi", "inmemory", "UseInMemory", null },
        { "avtobus-webapi", "kafka", "UseKafka", "AvtoBus.Kafka" },
        { "avtobus-webapi", "redis", "UseRedis", "AvtoBus.Redis" },
    };

    [Theory]
    [MemberData(nameof(Transports))]
    public void Instantiates_template_and_selects_transport(
        string shortName, string transport, string expectedBus, string? expectedExtraPackage)
    {
        using var work = new WorkDir();

        var nupkg = PackTemplates(work);
        Install(nupkg, work);

        var outDir = Path.Combine(work.Root, "app");
        Run("dotnet", null, work.Root, "new", shortName, "-n", "App", "-o", outDir,
            "--transport", transport, "--debug:custom-hive", work.Hive);

        var program = File.ReadAllText(Path.Combine(outDir, "Program.cs"));
        Assert.Contains($"bus.{expectedBus}(", program, StringComparison.Ordinal);
        Assert.DoesNotContain("#if", program, StringComparison.Ordinal);
        Assert.DoesNotContain("#elif", program, StringComparison.Ordinal);
        Assert.DoesNotContain("#else", program, StringComparison.Ordinal);
        Assert.DoesNotContain("#endif", program, StringComparison.Ordinal);

        var csproj = Directory.GetFiles(outDir, "*.csproj").Single();
        var project = File.ReadAllText(csproj);
        Assert.Contains("AvtoBus", project, StringComparison.Ordinal);
        if (expectedExtraPackage is null)
        {
            Assert.DoesNotContain("AvtoBus.Kafka", project, StringComparison.Ordinal);
            Assert.DoesNotContain("AvtoBus.Redis", project, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains(expectedExtraPackage, project, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("avtobus-worker", "inmemory")]
    [InlineData("avtobus-webapi", "inmemory")]
    public void Generated_project_builds_against_local_feed(string shortName, string transport)
    {
        using var work = new WorkDir();

        var nupkg = PackTemplates(work);
        Install(nupkg, work);
        var feed = PackLocalFeed(work);

        var outDir = Path.Combine(work.Root, "app");
        Run("dotnet", null, work.Root, "new", shortName, "-n", "App", "-o", outDir,
            "--transport", transport, "--debug:custom-hive", work.Hive);
        WriteNuGetConfig(outDir, feed);

        var r = Run("dotnet", outDir, null, "build", "-c", "Release", "--nologo", "-v", "minimal");
        Assert.True(r.ExitCode == 0, r.Output);
    }

    private static string PackTemplates(WorkDir work)
    {
        var r = Run("dotnet", null, RepoRoot,
            "pack", Path.Combine(RepoRoot, "src", "AvtoBus.Templates", "AvtoBus.Templates.csproj"),
            "-c", "Release", "-o", work.Templates, "--nologo");
        Assert.True(r.ExitCode == 0, r.Output);
        return Directory.GetFiles(work.Templates, "*.nupkg").Single();
    }

    private static void Install(string nupkg, WorkDir work)
    {
        var r = Run("dotnet", null, RepoRoot, "new", "install", nupkg, "--debug:custom-hive", work.Hive);
        Assert.True(r.ExitCode == 0, r.Output);
    }

    private static string PackLocalFeed(WorkDir work)
    {
        Directory.CreateDirectory(work.Feed);

        foreach (var project in new[]
                 {
                     Path.Combine(RepoRoot, "src", "AvtoBus", "AvtoBus.csproj"),
                     Path.Combine(RepoRoot, "src", "AvtoBus.Core", "AvtoBus.Core.csproj"),
                     Path.Combine(RepoRoot, "src", "AvtoBus.InMemory", "AvtoBus.InMemory.csproj"),
                 })
        {
            var r = Run("dotnet", null, RepoRoot, "pack", project, "-c", "Release", "-o", work.Feed, "--nologo");
            Assert.True(r.ExitCode == 0, r.Output);
        }

        return work.Feed;
    }

    private static void WriteNuGetConfig(string projectDir, string feed)
    {
        var config = $"""
            <?xml version="1.0" encoding="utf-8"?>
            <configuration>
              <packageSources>
                <clear />
                <add key="local" value="{feed}" />
                <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
              </packageSources>
            </configuration>
            """;
        File.WriteAllText(Path.Combine(projectDir, "nuget.config"), config);
    }

    private static (int ExitCode, string Output) Run(
        string fileName, string? workingDirectory, string? repoRoot,
        params string[] arguments)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            WorkingDirectory = workingDirectory ?? repoRoot ?? RepoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        foreach (var arg in arguments)
            psi.ArgumentList.Add(arg);

        psi.Environment["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1";
        psi.Environment["DOTNET_NOLOGO"] = "1";
        psi.Environment["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1";
        psi.Environment["DOTNET_CLI_UI_LANGUAGE"] = "en";

        using var process = Process.Start(psi)!;
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        return (process.ExitCode, output + error);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "AvtoBus.slnx")))
                return dir.FullName;
            dir = dir.Parent;
        }

        throw new InvalidOperationException("Корень репозитория (AvtoBus.slnx) не найден.");
    }

    private sealed class WorkDir : IDisposable
    {
        public WorkDir()
        {
            Root = Path.Combine(Path.GetTempPath(), "avtobus-template-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string Hive => Path.Combine(Root, "hive");

        public string Templates => Path.Combine(Root, "templates");

        public string Feed => Path.Combine(Root, "feed");

        public void Dispose()
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch
            {
                // Захваченные dotnet процессы могут удерживать файлы — не критично для теста.
            }
        }
    }
}
