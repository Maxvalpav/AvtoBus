using System.Reflection;

namespace AvtoBus.CliTests;

/// <summary>Контракты тестовой сборки (в этом же проекте).</summary>
public class PlaceOrder : AvtoBus.ICommand
{
    public required string OrderId { get; init; }
    public decimal Total { get; init; }
}

public class OrderPlaced : AvtoBus.IEvent
{
    public required string OrderId { get; init; }
}

/// <summary>Вызывает CLI, перехватывая stdout.</summary>
public static class CliRunner
{
    public static (int Code, string Output) Run(params string[] args)
    {
        var original = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        var exitCode = AvtoBus.Cli.Program.Main(args).GetAwaiter().GetResult();

        Console.SetOut(original);
        return (exitCode, writer.ToString());
    }

    public static string ThisAssembly => typeof(PlaceOrder).Assembly.Location;
}

public class CliSmokeTests
{
    [Fact]
    public void Doctor_reports_runtime_and_exits_zero()
    {
        var (code, output) = CliRunner.Run("doctor");

        Assert.Equal(0, code);
        Assert.Contains("AvtoBus doctor", output);
        Assert.Contains("Ядро: v", output);
    }

    [Fact]
    public void Contracts_lists_contracts_from_assembly()
    {
        var (code, output) = CliRunner.Run("contracts", "--assembly", CliRunner.ThisAssembly);

        Assert.Equal(0, code);
        Assert.Contains("place-order", output);
        Assert.Contains("order-placed", output);
    }

    [Fact]
    public void Contracts_json_is_parseable()
    {
        var (code, output) = CliRunner.Run("contracts", "--assembly", CliRunner.ThisAssembly, "--format", "json");

        Assert.Equal(0, code);
        using var doc = System.Text.Json.JsonDocument.Parse(output);
        Assert.True(doc.RootElement.GetProperty("count").GetInt32() >= 2);
    }

    [Fact]
    public void Es_explain_overview_lists_events_and_commands()
    {
        var (code, output) = CliRunner.Run("es", "explain", "--assembly", CliRunner.ThisAssembly);

        Assert.Equal(0, code);
        Assert.Contains("Событий:", output);
        Assert.Contains("Команд:", output);
    }

    [Fact]
    public void Es_explain_contract_shows_fields()
    {
        var (code, output) = CliRunner.Run(
            "es", "explain", "--assembly", CliRunner.ThisAssembly, "--contract", "PlaceOrder");

        Assert.Equal(0, code);
        Assert.Contains("команда: PlaceOrder", output);
        Assert.Contains("OrderId", output);
    }

    [Fact]
    public void Completion_generates_zsh_script()
    {
        var (code, output) = CliRunner.Run("completion", "zsh");

        Assert.Equal(0, code);
        Assert.Contains("#compdef avtobus", output);
    }

    [Fact]
    public void Unknown_command_returns_error()
    {
        var (code, _) = CliRunner.Run("definitely-not-a-command");

        Assert.Equal(1, code);
    }

    [Fact]
    public void No_args_shows_help()
    {
        var (code, output) = CliRunner.Run();

        Assert.Equal(0, code);
        Assert.Contains("doctor", output);
        Assert.Contains("es explain", output);
    }
}
