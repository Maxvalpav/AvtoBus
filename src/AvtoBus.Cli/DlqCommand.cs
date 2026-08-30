using System.CommandLine;
using System.CommandLine.Parsing;

namespace AvtoBus.Cli;

/// <summary>
/// Каркас DLQ-команд. Полная версия требует management API транспортов
/// (TransportAdmin) — здесь честный статус и файловый формат для оффлайн-данных.
/// </summary>
public static class DlqCommand
{
    public static Command Create()
    {
        var command = new Command("dlq", "Просмотр dead-letter сообщений");

        command.Add(ListCommand());
        command.Add(StatusCommand());
        return command;
    }

    private static Command ListCommand()
    {
        var command = new Command("list", "Список DLQ-сообщений из файла (AVTOBUS_DLQ_FILE)");

        var file = new Option<string>("--file") { DefaultValueFactory = _ => Environment.GetEnvironmentVariable("AVTOBUS_DLQ_FILE") ?? "dlq.jsonl" };
        command.Add(file);

        command.SetAction(async (parseResult, ct) =>
        {
            var filePath = parseResult.GetValue(file) ?? "dlq.jsonl";
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Файл DLQ не найден: {filePath}");
                Console.WriteLine("Полная версия читает DLQ транспорта напрямую через management API (в разработке).");
                return 0;
            }

            var count = 0;
            await foreach (var line in File.ReadLinesAsync(filePath))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                count++;
                var message = line.Length > 120 ? line[..120] + "…" : line;
                Console.WriteLine($"  {count,3}  {message}");
            }

            Console.WriteLine($"Итого: {count} сообщений");
            return 0;
        });

        return command;
    }

    private static Command StatusCommand()
    {
        var command = new Command("status", "Статус DLQ-интеграции");

        command.SetAction((parseResult, ct) =>
        {
            Console.WriteLine("DLQ status");
            Console.WriteLine("  Интеграция с реальными очередями транспорта: не подключена (требует management API).");
            Console.WriteLine("  Файловый формат (JSONL): готов через 'dlq list --file'.");
            Console.WriteLine("  Очередь в проде: см. README 'DLQ' — сообщения попадают в *.error-очередь транспорта.");
            return Task.FromResult(0);
        });

        return command;
    }
}
