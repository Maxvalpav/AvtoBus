using System.CommandLine;
using System.Reflection;
using System.Text.Json;

namespace AvtoBus.Cli;

public static class AsyncApiCommand
{
    public static Command Create()
    {
        var command = new Command("asyncapi", "Генерация AsyncAPI 3.0 из контрактов сборки");

        var assembly = new Option<string>("--assembly") { Description = "Путь к сборке с контрактами" };
        var output = new Option<string>("--output") { Description = "Путь к выходному asyncapi.json (по умолчанию stdout)" };
        var title = new Option<string>("--title") { DefaultValueFactory = _ => "AvtoBus API", Description = "Заголовок документа" };
        var version = new Option<string>("--version") { DefaultValueFactory = _ => "1.0.0", Description = "Версия" };

        command.Add(assembly);
        command.Add(output);
        command.Add(title);
        command.Add(version);

        command.SetAction(async (parseResult, ct) =>
        {
            var asmPath = parseResult.GetValue(assembly);
            var outPath = parseResult.GetValue(output);
            var docTitle = parseResult.GetValue(title) ?? "AvtoBus API";
            var docVersion = parseResult.GetValue(version) ?? "1.0.0";

            var asm = ResolveAssembly(asmPath);
            var contracts = ContractScanner.Scan(asm);

            var doc = new Dictionary<string, object>
            {
                ["asyncapi"] = "3.0.0",
                ["info"] = new Dictionary<string, object> { ["title"] = docTitle, ["version"] = docVersion },
                ["defaultContentType"] = "application/json",
                ["channels"] = BuildChannels(contracts),
                ["operations"] = BuildOperations(contracts),
                ["components"] = new Dictionary<string, object>
                {
                    ["messages"] = BuildMessages(contracts),
                    ["schemas"] = BuildSchemas(contracts),
                },
            };

            var json = JsonSerializer.Serialize(doc, new JsonSerializerOptions { WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
            if (outPath is not null)
            {
                await File.WriteAllTextAsync(outPath, json, ct);
                Console.WriteLine($"AsyncAPI: {contracts.Count} контрактов → {outPath}");
            }
            else
            {
                Console.WriteLine(json);
            }
            return 0;
        });

        return command;
    }

    private static Dictionary<string, object> BuildChannels(IReadOnlyList<Type> types)
    {
        var d = new Dictionary<string, object>();
        foreach (var t in types)
        {
            var ch = MessageTypeNaming.NameOf(t);
            d[ch] = new Dictionary<string, object> { ["address"] = ch, ["messages"] = new Dictionary<string, object> { [t.Name] = new Dictionary<string, object> { ["$ref"] = $"#/components/messages/{t.Name}" } } };
        }
        return d;
    }

    private static Dictionary<string, object> BuildOperations(IReadOnlyList<Type> types)
    {
        var d = new Dictionary<string, object>();
        foreach (var t in types)
        {
            var ch = MessageTypeNaming.NameOf(t);
            var isCmd = typeof(ICommand).IsAssignableFrom(t);
            d[$"{(isCmd ? "send" : "publish")}_{t.Name}"] = new Dictionary<string, object>
            {
                ["action"] = isCmd ? "send" : "receive",
                ["channel"] = new Dictionary<string, object> { ["$ref"] = $"#/channels/{ch}" },
            };
        }
        return d;
    }

    private static Dictionary<string, object> BuildMessages(IReadOnlyList<Type> types)
    {
        var d = new Dictionary<string, object>();
        foreach (var t in types)
            d[t.Name] = new Dictionary<string, object> { ["name"] = MessageTypeNaming.NameOf(t), ["title"] = t.Name, ["contentType"] = "application/json" };
        return d;
    }

    private static Dictionary<string, object> BuildSchemas(IReadOnlyList<Type> types)
    {
        var d = new Dictionary<string, object>();
        foreach (var t in types)
        {
            var props = new Dictionary<string, object>();
            foreach (var p in t.GetProperties().Where(p => p.GetIndexParameters().Length == 0))
                props[p.Name] = new Dictionary<string, object> { ["type"] = MapType(p.PropertyType) };
            d[t.Name] = new Dictionary<string, object> { ["type"] = "object", ["properties"] = props };
        }
        return d;
    }

    private static string MapType(Type t)
        => t == typeof(string) || t == typeof(Guid) ? "string" : t == typeof(int) || t == typeof(long) ? "integer" : t == typeof(bool) ? "boolean" : t == typeof(decimal) || t == typeof(double) ? "number" : "object";

    private static Assembly ResolveAssembly(string? path)
    {
        if (path is null) return typeof(IBus).Assembly;
        var full = Path.GetFullPath(path);
        if (!File.Exists(full)) throw new FileNotFoundException($"Сборка не найдена: {full}");
        return Assembly.LoadFrom(full);
    }
}
