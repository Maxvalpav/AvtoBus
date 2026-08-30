using System.CommandLine;
using System.CommandLine.Parsing;
using System.Reflection;

namespace AvtoBus.Cli;

public static class DoctorCommand
{
    public static Command Create()
    {
        var command = new Command("doctor", "Диагностика окружения и конфига");

        var assembly = new Option<string>("--assembly") { Description = "Сборка с контрактами (по умолчанию — CLI)" };
        var check = new Option<bool>("--check") { Description = "Вернуть ненулевой код при проблемах" };

        command.Add(assembly);
        command.Add(check);

        command.SetAction((parseResult, ct) =>
        {
            var assemblyPath = parseResult.GetValue(assembly);
            var checkOnly = parseResult.GetValue(check);
            var failures = 0;

            Console.WriteLine("AvtoBus doctor");

            var runtime = typeof(IBus).Assembly.GetName().Version?.ToString() ?? "?";
            Console.WriteLine($"  [ok]  Ядро: v{runtime}");

            var config = CliConfig.Load();
            var configExists = File.Exists(CliConfig.ConfigPath);
            Console.WriteLine($"  [{(configExists ? "ok" : "warn")}]  Конфиг: {(configExists ? CliConfig.ConfigPath : "не найден — создастся при настройке")}");

            var connectionOk = !string.IsNullOrEmpty(config.ConnectionString);
            Console.WriteLine($"  [{(connectionOk ? "ok" : "warn")}]  Connection: {(connectionOk ? "задан" : "не задан")}");
            if (!connectionOk)
                failures++;

            if (assemblyPath is not null)
            {
                if (!File.Exists(assemblyPath))
                {
                    Console.WriteLine($"  [FAIL]  Сборка не найдена: {assemblyPath}");
                    failures++;
                }
                else
                {
                    var asm = Assembly.LoadFrom(Path.GetFullPath(assemblyPath));
                    var contracts = ContractScanner.Scan(asm);
                    Console.WriteLine($"  [ok]  Сборка {asm.GetName().Name}: {contracts.Count} контрактов");
                }
            }

            Console.WriteLine(checkOnly && failures > 0 ? "doctor: проблемы найдены" : "doctor: готово");
            return Task.FromResult(checkOnly && failures > 0 ? 1 : 0);
        });

        return command;
    }
}
