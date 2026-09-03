namespace AvtoBus.Cli;

/// <summary>Конфиг CLI: ~/.config/avtobus/config.json.</summary>
public sealed class CliConfig
{
    public string? DefaultConnection { get; set; }
    public string? DefaultFormat { get; set; } = "table";
    public string? DefaultAssembly { get; set; }

    public string? ConnectionString { get; set; }
    public string? Transport { get; set; } = TransportNames.InMemory;

    public static string ConfigPath
    {
        get
        {
            var baseDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            return Path.Combine(baseDir, ".config", "avtobus", "config.json");
        }
    }

    public static CliConfig Load()
    {
        if (!File.Exists(ConfigPath))
            return new CliConfig();

        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<CliConfig>(File.ReadAllText(ConfigPath)) ?? new CliConfig();
        }
        catch
        {
            return new CliConfig();
        }
    }

    public void Save()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(ConfigPath)!);
        File.WriteAllText(ConfigPath, System.Text.Json.JsonSerializer.Serialize(this, new System.Text.Json.JsonSerializerOptions
        {
            WriteIndented = true,
        }));
        // В конфиге лежит connection string — ограничиваем права до owner-only (best effort).
        try
        {
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(ConfigPath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        catch { }
    }
}
