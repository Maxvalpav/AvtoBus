using System.CommandLine;

namespace AvtoBus.Cli;

/// <summary>Аварийный режим «только чтение» (идея 497): флаг в ~/.config/avtobus/readonly.</summary>
public static class ReadonlyCommand
{
    private static string FlagPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config", "avtobus", "readonly");

    public static Command Create()
    {
        var cmd = new Command("readonly", "Аварийный режим: avtobus readonly on|off|status (идея 497)");
        var stateArg = new Argument<string?>("state") { Description = "on|off|status", Arity = ArgumentArity.ZeroOrOne, DefaultValueFactory = _ => "status" };
        cmd.Add(stateArg);
        cmd.SetAction((parseResult, _) =>
        {
            var state = parseResult.GetValue(stateArg) ?? "status";
            switch (state.ToLowerInvariant())
            {
                case "on":
                    Directory.CreateDirectory(Path.GetDirectoryName(FlagPath)!);
                    File.WriteAllText(FlagPath, "on:" + DateTimeOffset.UtcNow.ToString("O"));
                    Console.WriteLine($"readonly ON → {FlagPath} (BusOptions.IsReadOnly = true)");
                    break;
                case "off":
                    if (File.Exists(FlagPath)) File.Delete(FlagPath);
                    Console.WriteLine("readonly OFF");
                    break;
                case "status":
                    Console.WriteLine(File.Exists(FlagPath)
                        ? $"readonly ON ({File.ReadAllText(FlagPath)})"
                        : "readonly OFF");
                    break;
                default:
                    Console.Error.WriteLine("use: avtobus readonly [on|off|status]");
                    Environment.ExitCode = 1;
                    break;
            }
            return Task.FromResult(0);
        });
        return cmd;
    }

    public static bool IsReadOnlyFlagSet() => File.Exists(FlagPath) || Environment.GetEnvironmentVariable("AVTOBUS_READONLY") == "1";
}
