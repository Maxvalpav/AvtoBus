using System.CommandLine;
using System.CommandLine.Parsing;
using System.Reflection;

namespace AvtoBus.Cli;

public static class ContractsCommand
{
    public static Command Create()
    {
        var command = new Command("contracts", "Сканирование контрактов из сборки");

        var assembly = new Option<string>("--assembly") { Description = "Путь к сборке с контрактами (по умолчанию — сборка CLI)" };
        var format = new Option<string>("--format") { DefaultValueFactory = _ => "table", HelpName = "table|json" };

        command.Add(assembly);
        command.Add(format);

        command.SetAction(async (parseResult, ct) =>
        {
            var assemblyPath = parseResult.GetValue(assembly);
            var fmt = parseResult.GetValue(format) ?? "table";

            var asm = ResolveAssembly(assemblyPath);
            var contracts = ContractScanner.Scan(asm);

            if (fmt == "json")
            {
                var doc = new
                {
                    assembly = asm.GetName().Name,
                    count = contracts.Count,
                    contracts = contracts.Select(t => new
                    {
                        name = MessageTypeNaming.NameOf(t),
                        clrType = t.FullName,
                        kind = typeof(ICommand).IsAssignableFrom(t) ? "command" : "event",
                    }),
                };
                Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(doc, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true,
                }));
                return 0;
            }

            Console.WriteLine($"Сборка: {asm.GetName().Name}");
            Console.WriteLine($"Контрактов: {contracts.Count}");
            Console.WriteLine();
            foreach (var type in contracts)
            {
                var kind = typeof(ICommand).IsAssignableFrom(type) ? "cmd " : "evt ";
                Console.WriteLine($"  {kind} {MessageTypeNaming.NameOf(type),-48} {type.Name}");
            }
            return 0;
        });

        return command;
    }

    private static Assembly ResolveAssembly(string? path)
    {
        if (path is null)
            return typeof(IBus).Assembly;

        var full = Path.GetFullPath(path);
        if (!File.Exists(full))
            throw new FileNotFoundException($"Сборка не найдена: {full}");

        return Assembly.LoadFrom(full);
    }
}
