namespace AvtoBus.CliTests;

/// <summary>Регрессия: загрузчик сборок изолирован, --output не затирается молча.</summary>
public class AssemblyLoaderTests
{
    [Fact]
    public void Rejects_urls_and_non_assemblies()
    {
        Assert.Throws<InvalidOperationException>(
            () => AvtoBus.Cli.AssemblyLoader.LoadContractsAssembly("https://example.com/x.dll"));
        Assert.Throws<InvalidOperationException>(
            () => AvtoBus.Cli.AssemblyLoader.LoadContractsAssembly("notes.txt"));
    }

    [Fact]
    public void Loads_local_assembly_for_scanning()
    {
        var asm = AvtoBus.Cli.AssemblyLoader.LoadContractsAssembly(CliRunner.ThisAssembly);
        Assert.NotNull(asm);
        Assert.NotEmpty(asm.GetTypes());
    }
}

/// <summary>Регрессия: asyncapi --output требует --force для перезаписи. Console — в [Collection("cli")].</summary>
[Collection("cli")]
public class AsyncApiForceTests
{
    [Fact]
    public void Output_refuses_overwrite_without_force()
    {
        var path = Path.Combine(Path.GetTempPath(), $"avtobus-{Guid.NewGuid():N}.json");
        var originalError = Console.Error;
        using var errWriter = new StringWriter();
        try
        {
            var (first, _) = CliRunner.Run(
                "asyncapi", "--assembly", CliRunner.ThisAssembly, "--output", path);
            Assert.Equal(0, first);
            Assert.True(File.Exists(path));

            Console.SetError(errWriter);
            var (second, _) = CliRunner.Run(
                "asyncapi", "--assembly", CliRunner.ThisAssembly, "--output", path);
            Assert.Equal(2, second);
            Assert.Contains("--force", errWriter.ToString(), StringComparison.Ordinal);

            var (third, _) = CliRunner.Run(
                "asyncapi", "--assembly", CliRunner.ThisAssembly, "--output", path, "--force");
            Assert.Equal(0, third);
        }
        finally
        {
            Console.SetError(originalError);
            if (File.Exists(path)) File.Delete(path);
        }
    }
}
