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
                Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(config, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true,
                }));
                return Task.FromResult(0);
            }

            Console.WriteLine($"Файл:        {CliConfig.ConfigPath}");
            Console.WriteLine($"Транспорт:   {config.Transport}");
            Console.WriteLine($"Connection:  {config.ConnectionString ?? "(не задан)"}");
            Console.WriteLine($"Формат:      {config.DefaultFormat}");
            Console.WriteLine($"Сборка:      {config.DefaultAssembly ?? "(не задана)"}");
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
        command.Add(setConnection);
        return command;
    }
}
