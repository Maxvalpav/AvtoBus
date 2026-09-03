using System.CommandLine;
using System.CommandLine.Parsing;
using System.Reflection;

namespace AvtoBus.Cli;

public static class EsCommand
{
    public static Command Create()
    {
        var command = new Command("es", "Event Sourcing: объяснение модели из контрактов");

        command.Add(ExplainCommand());
        return command;
    }

    private static Command ExplainCommand()
    {
        var command = new Command("explain", "Объяснить ES-модель: контракты, потоки, агрегаты, проекции");

        var assembly = new Option<string>("--assembly") { Description = "Путь к сборке с контрактами (по умолчанию — сборка CLI)" };
        var contract = new Option<string>("--contract") { Description = "Конкретное событие/команда для детального объяснения" };

        command.Add(assembly);
        command.Add(contract);

        command.SetAction((parseResult, ct) =>
        {
            var assemblyPath = parseResult.GetValue(assembly);
            var contractName = parseResult.GetValue(contract);

            var asm = ResolveAssembly(assemblyPath);
            var contracts = ContractScanner.Scan(asm);

            if (contractName is null)
            {
                PrintOverview(contracts);
                return Task.FromResult(0);
            }

            var type = ContractScanner.Resolve(contracts, contractName);
            if (type is null)
            {
                Console.WriteLine($"Контракт '{contractName}' не найден в {asm.GetName().Name}.");
                PrintOverview(contracts);
                return Task.FromResult(0);
            }

            PrintContract(type);
            return Task.FromResult(0);
        });

        return command;
    }

    private static void PrintOverview(IReadOnlyList<Type> contracts)
    {
        var events = contracts.Where(t => typeof(IEvent).IsAssignableFrom(t)).ToArray();
        var commands = contracts.Where(t => typeof(ICommand).IsAssignableFrom(t)).ToArray();

        Console.WriteLine("Event Sourcing — модель из контрактов");
        Console.WriteLine();
        Console.WriteLine($"Событий:  {events.Length}");
        Console.WriteLine($"Команд:   {commands.Length}");
        Console.WriteLine();
        Console.WriteLine("Как это работает:");
        Console.WriteLine("  1. Команда (ICommand) валидируется и превращается в события (IEvent).");
        Console.WriteLine("  2. События дописываются в Event Store потоком агрегата (stream).");
        Console.WriteLine("  3. Агрегат восстанавливает состояние реплеем своих событий (Evolve).");
        Console.WriteLine("  4. Проекции читают глобальный поток (global_seq) и строят read-модели.");
        Console.WriteLine("  5. ProjectionManager умеет реплеить и переключать версии blue/green.");
        Console.WriteLine("  6. SubjectDataProtection шифрует поля субъекта; Forget = «право на забвение».");
        Console.WriteLine();
        Console.WriteLine("События (кандидаты в доменные события потока):");
        foreach (var e in events.Take(10))
            Console.WriteLine($"  • {MessageTypeNaming.NameOf(e)}  ({e.Name})");

        Console.WriteLine();
        Console.WriteLine($"Команды ({commands.Length}):");
        foreach (var c in commands.Take(10))
            Console.WriteLine($"  • {MessageTypeNaming.NameOf(c)}  ({c.Name})");

        Console.WriteLine();
        Console.WriteLine("Используйте 'avtobus es explain --contract <имя>' для деталей контракта.");
    }

    private static void PrintContract(Type type)
    {
        var kind = typeof(ICommand).IsAssignableFrom(type) ? "команда" : "событие";
        Console.WriteLine($"{kind}: {type.Name}");
        Console.WriteLine($"  Имя на проводе: {MessageTypeNaming.NameOf(type)}");
        Console.WriteLine($"  CLR:            {type.FullName}");
        Console.WriteLine($"  Namespace:      {type.Namespace}");
        Console.WriteLine();
        Console.WriteLine("Поля:");
        foreach (var prop in type.GetProperties())
        {
            Console.WriteLine($"  {prop.PropertyType.Name,-20} {prop.Name}");
        }
        Console.WriteLine();
        Console.WriteLine(kind == "событие"
            ? "Это событие: в ES-модели оно immutable, дописывается в поток и питает проекции."
            : "Это команда: в ES-модели она валидируется и сворачивается в события (Decider).");
    }

    private static Assembly ResolveAssembly(string? path) => AssemblyLoader.LoadContractsAssembly(path);
}
