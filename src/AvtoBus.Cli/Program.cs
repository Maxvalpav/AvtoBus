using System.CommandLine;
using System.CommandLine.Invocation;

namespace AvtoBus.Cli;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var root = new RootCommand("AvtoBus — командная строка для шины")
        {
            DoctorCommand.Create(),
            ContractsCommand.Create(),
            EsCommand.Create(),
            ConfigCommand.Create(),
            CompletionCommand.Create(),
            DlqCommand.Create(),
            AsyncApiCommand.Create(),
            ReadonlyCommand.Create(),
        };

        if (args.Length == 0)
        {
            WriteHelp(root);
            return 0;
        }

        var parseResult = root.Parse(args);
        if (parseResult.Errors.Count > 0)
        {
            foreach (var error in parseResult.Errors)
                Console.Error.WriteLine(error.Message);
            return 1;
        }

        var configuration = new InvocationConfiguration();
        return await parseResult.InvokeAsync(configuration, CancellationToken.None);
    }

    private static void WriteHelp(RootCommand root)
    {
        Console.WriteLine("AvtoBus CLI — команды:");
        Console.WriteLine("  doctor        — диагностика окружения и конфига");
        Console.WriteLine("  contracts     — сканирование контрактов из сборки");
        Console.WriteLine("  es explain    — объяснение Event Sourcing модели из контрактов");
        Console.WriteLine("  config        — управление конфигом ~/.config/avtobus");
        Console.WriteLine("  dlq           — просмотр dead-letter сообщений (in-memory/файл)");
        Console.WriteLine("  readonly      — avtobus readonly on|off|status (идея 497)");
        Console.WriteLine("  completion    — генерация shell-автодополнения");
        Console.WriteLine("  asyncapi      — генерация AsyncAPI 3.0 из контрактов");
        Console.WriteLine();
        Console.WriteLine("Используйте 'avtobus <команда> --help' для деталей.");
    }
}
