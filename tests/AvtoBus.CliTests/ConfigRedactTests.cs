namespace AvtoBus.CliTests;

/// <summary>Регрессия аудита: `config show` не должен светить секреты.</summary>
[Collection("cli")] // CliRunner переключает Console.Out — со CliSmokeTests только последовательно
public class ConfigRedactTests
{
    [Theory]
    [InlineData("amqp://guest:guest@localhost:5672/", "amqp://guest:***@localhost:5672/")]
    [InlineData("Host=db;Username=app;Password=s3cret;Database=bus", "Host=db;Username=app;Password=***;Database=bus")]
    [InlineData("postgres://bob:hunter2@db:5432/app", "postgres://bob:***@db:5432/app")]
    public void Redact_masks_secrets(string input, string expected)
    {
        Assert.Equal(expected, AvtoBus.Cli.ConfigCommand.Redact(input));
    }

    [Fact]
    public void Redact_keeps_secretless_values()
    {
        Assert.Equal("inmemory://localhost", AvtoBus.Cli.ConfigCommand.Redact("inmemory://localhost"));
        Assert.Null(AvtoBus.Cli.ConfigCommand.Redact(null));
    }

    [Fact]
    public void Config_show_does_not_print_password()
    {
        var (code, output) = CliRunner.Run("config", "show");
        Assert.Equal(0, code);
        // Даже если в конфиге лежит реальный пароль, в выводе его быть не должно.
        Assert.DoesNotContain("guest:guest@", output);
    }
}
