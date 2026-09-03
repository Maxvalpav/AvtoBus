using System.CommandLine;
using System.CommandLine.Parsing;

namespace AvtoBus.Cli;

public static class ConfigCommand
{
    public static Command Create()
    {
        var command = new Command("config", "Управление конфигом ~/.config/avtobus/config.json");

        var show = new Command("show", "Показать текущий конфиг");
        var format = new Option<string>("--format") { DefaultValueFactory = _ => "table", HelpName = "table|json" };
        show.Add(format);
        show.SetAction((parseResult, ct) =>
        {
            var fmt = parseResult.GetValue(format) ?? "table";
            var config = CliConfig.Load();

            if (fmt == "json")
            {
                var redacted = new CliConfig
                {
                    DefaultConnection = config.DefaultConnection,
                    DefaultFormat = config.DefaultFormat,
                    DefaultAssembly = config.DefaultAssembly,
                    ConnectionString = Redact(config.ConnectionString),
                    Transport = config.Transport,
                };
                Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(redacted, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true,
                }));
                return Task.FromResult(0);
            }

            Console.WriteLine($"Файл:        {CliConfig.ConfigPath}");
            Console.WriteLine($"Транспорт:   {config.Transport}");
            Console.WriteLine($"Connection:  {Redact(config.ConnectionString) ?? "(не задан)"}");
            Console.WriteLine($"Формат:      {config.DefaultFormat}");
            Console.WriteLine($"Сборка:      {config.DefaultAssembly ?? "(не задана)"}");
            return Task.FromResult(0);
        });

        var showSecret = new Command("show-secret", "Показать connection string целиком (осторожно: попадёт в историю shell)");
        showSecret.SetAction((_, _) =>
        {
            Console.WriteLine(CliConfig.Load().ConnectionString ?? "(не задан)");
            return Task.FromResult(0);
        });

        var setConnection = new Command("set-connection", "Задать connection string");
        var connection = new Argument<string>("connection");
        setConnection.Add(connection);
        setConnection.SetAction((parseResult, ct) =>
        {
            var value = parseResult.GetValue(connection);
            var config = CliConfig.Load();
            config.ConnectionString = value;
            config.Save();
            Console.WriteLine($"Сохранено в {CliConfig.ConfigPath}");
            return Task.FromResult(0);
        });

        command.Add(show);
        command.Add(showSecret);
        command.Add(setConnection);
        return command;
    }

    /// <summary>
    /// Маскирует секреты в connection string: пароль/токен заменяются на ***.
    /// Полное значение — только через `config show-secret`.
    /// </summary>
    public static string? Redact(string? connectionString)
    {
        if (string.IsNullOrEmpty(connectionString)) return connectionString;
        var s = connectionString;
        // URI-форма: scheme://user:password@host...
        var at = s.IndexOf('@');
        var scheme = s.IndexOf("://", StringComparison.Ordinal);
        if (at > 0 && scheme >= 0 && scheme < at)
        {
            var colon = s.LastIndexOf(':', at - 1, at - scheme - 3);
            if (colon > scheme + 2)
                s = s[..(colon + 1)] + "***" + s[at..];
        }
        // KV-форма: Password=...; / Pwd=... — маскируем значение до ';'.
        foreach (var key in new[] { "Password", "Pwd" })
        {
            var idx = s.IndexOf(key + "=", StringComparison.OrdinalIgnoreCase);
            if (idx < 0) continue;
            var start = idx + key.Length + 1;
            var end = s.IndexOf(';', start);
            s = end < 0 ? s[..start] + "***" : s[..start] + "***" + s[end..];
        }
        return s;
    }
}
