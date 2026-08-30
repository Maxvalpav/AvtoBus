# ⌨ Реализация: CLI `avtobus` (dotnet tool)

> **Design draft.** Команды CLI ещё не собраны в `dotnet tool`; синтаксис может измениться после появления management API.

Целевой сценарий: `dotnet tool install AvtoBus.Cli -g` → одна команда `avtobus`.

## 1. Корневой команд-роутер (System.CommandLine)

```csharp
// AvtoBus.Cli/Program.cs
return await new RootCommand("AvtoBus — command-line for the bus")
{
    Commands.Doctor,
    Commands.Topology,
    Commands.Dlq,
    Commands.Saga,
    Commands.Projections,
    Commands.Contracts,
    Commands.Schema,
    Commands.Dev,
    Commands.Bench,
    Commands.Completion,
}.InvokeAsync(args);
```

## 2. `avtobus doctor` — диагностика

```csharp
public static Command Doctor { get; } = new("doctor", "Check environment, brokers, outbox tables, clocks")
{
    Handler = CommandHandler.Create(async (
        string? connectionString,
        string? broker,
        IConsole console) =>
    {
        var results = new List<HealthCheck>();

        // 1. Брокер
        if (broker is not null)
        {
            var r = await CheckBroker(broker);
            results.Add(r);
        }

        // 2. NTP skew (относительно брокера/systemd-timesyncd)
        var skew = await MeasureNtpSkew();
        results.Add(new("Clock skew",
            Math.Abs(skew) < TimeSpan.FromSeconds(1),
            $"skew = {skew.TotalMilliseconds:F0} ms"));

        // 3. Outbox-таблицы
        if (connectionString is not null)
        {
            var cs = await CheckOutboxSchema(connectionString);
            results.Add(cs);
        }

        // 4. Версии транспорта/ядра
        results.Add(new("AvtoBus runtime", true, FileVersionInfo.GetVersionInfo(typeof(IBus).Assembly.Location).ProductVersion!));

        // Таблица вывода
        var ok = true;
        foreach (var r in results)
        {
            console.Write($"  [{(r.Ok ? "ok" : "✗ ")}] ");
            console.ForegroundColor = r.Ok ? ConsoleColor.Green : ConsoleColor.Red;
            console.WriteLine($"{r.Name}: {r.Detail}");
            console.ResetColor();
            ok &= r.Ok;
        }

        return ok ? 0 : 1;
    })
};

file sealed record HealthCheck(string Name, bool Ok, string Detail);
```

## 3. `avtobus topology apply/diff`

```csharp
var topology = new Command("topology", "Manage broker topology")
{
    new Command("apply", "Apply planned topology to the broker")
    {
        Handler = CommandHandler.Create(async (string transport, string config, bool dryRun, IConsole console) =>
        {
            var plan = TopologyLoader.FromConfig(config);
            using var admin = TransportAdminFactory.For(transport);
            var diff = await admin.DiffAsync(plan);

            console.WriteLine($"Queues   to create: {diff.QueuesToCreate.Count}");
            console.WriteLine($"Queues   to delete: {diff.QueuesToDelete.Count}");
            console.WriteLine($"Bindings to add:    {diff.BindingsToAdd.Count}");

            if (dryRun) return 0;
            await admin.ApplyAsync(diff);
            console.WriteLine("✅ Topology applied");
            return 0;
        })
    },
    new Command("export", "Export current broker topology") { /* ... */ },
};
```

Формат плана топологии как кода:

```yaml
# avtobus.topology.yaml
transports:
  rabbit: amqp://rabbit

queues:
  - name: orders
    type: quorum
    bindings:
      - exchange: orders.*
  - name: orders.error
    ttl: null # keep forever
    bindings: []

exchanges:
  - name: orders.order-placed
    type: topic
    durable: true
```

## 4. `avtobus dlq` — просмотр и реплей

```csharp
var dlq = new Command("dlq", "Inspect and replay dead-letter queues")
{
    new Command("list")
    {
        Handler = CommandHandler.Create<string>((queue) => /* */)
    },
    new Command("peek")
    {
        new Option<string>("--queue"),
        new Option<int>("--count", () => 20),
        new Option<string>("--format", () => "table") { Description = "table|json|jsonl" },
    },
    new Command("replay")
    {
        new Option<string>("--queue"),
        new Option<string>("--filter"),     // "type=PlaceOrder AND body.Total > 1000"
        new Option<int>("--rate", () => 10), // msg/s
        new Option<int>("--max", () => int.MaxValue),
        new Option<FileInfo?>("--patch", description: "JS file with (msg) => msg transform"),
        Handler = CommandHandler.Create<DlqReplayOptions>(Dlq.ReplayAsync)
    },
    new Command("edit", "Open selected message in $EDITOR, save to replay") { /* */ },
    new Command("archive", "Move DLQ to S3 with index") { /* */ },
};
```

Пример патча как JS (удобно для оперативных фиксов в проде):

```js
// fix-customerid.js
module.exports = (msg) => {
  msg.body.customerId = "cust-42";
  return msg;
};
```

```bash
avtobus dlq replay --queue orders.error --filter 'error=NullReferenceException' --patch ./fix-customerid.js --rate 5
```

## 5. `avtobus saga` — управление сагами

```csharp
var saga = new Command("saga", "Manage sagas")
{
    new Command("list")
    {
        new Option<string>("--type"),
        new Option<string>("--status"),
        new Option<TimeSpan?>("--stuck", () => null),
    },
    new Command("show")
    {
        new Option<Guid>("--id"),
        Handler = CommandHandler.Create<Guid>(async id =>
        {
            var vm = await sagaAdmin.Get(id);
            AnsiConsole.Write(new Panel($"""
                Type:      {vm.SagaType}
                Status:    {vm.Status}
                Correl:    {vm.CorrelationKey}
                Created:   {vm.CreatedAt}
                Updated:   {vm.UpdatedAt}
                Events:    {vm.EventCount}
                """));

            // Таймлайн с подсветкой (Spectre.Console)
            AnsiConsole.Write(new Timeline(vm.Events));
        })
    },
    new Command("pause") { new Option<Guid>("--id") },
    new Command("resume") { new Option<Guid>("--id") },
    new Command("cancel") { new Option<Guid>("--id"), new Option<string>("--reason") },
    new Command("retry")  { new Option<Guid>("--id") },
};
```

## 6. `avtobus projections`

```csharp
var projections = new Command("projections", "Manage projections")
{
    new Command("rebuild")
    {
        new Option<string>("--name"),
        new Option<int>("--parallel", () => 8),
        new Option<bool>("--keep-old", () => true), // blue/green
    },
    new Command("status") { /* выводит lag/progress */ },
};
```

## 7. `avtobus contracts`

```csharp
var contracts = new Command("contracts", "Work with contracts")
{
    new Command("export")
    {
        new Option<string>("--lang", () => "ts") { Description = "ts|java|go|python" },
        new Option<DirectoryInfo>("--out") { IsRequired = true },
    },
    new Command("asyncapi")
    {
        Handler = CommandHandler.Create(() =>
        {
            var doc = AsyncApiGenerator.Generate();
            Console.WriteLine(doc);
        })
    },
    new Command("bump")
    {
        new Option<string>("--contract") { IsRequired = true },
        Handler = CommandHandler.Create<string>(name => ContractBumper.Bump(name))
    },
    new Command("check")
    {
        Handler = CommandHandler.Create<string[]>(files => SchemaChecker.Check(files))
    },
};
```

## 8. `avtobus dev up` — локальная инфраструктура

```csharp
var dev = new Command("dev", "Local development")
{
    new Command("up", "Start infrastructure (Rabbit/Postgres/Jaeger/Redis)")
    {
        new Option<bool>("--reset", () => false, "wipe volumes"),
        new Option<string[]>("--with", () => ["rabbit", "postgres", "jaeger"]),
        Handler = CommandHandler.Create<DevUpOptions>(async opt =>
        {
            var file = Path.Combine(AppContext.BaseDirectory, "compose.dev.yaml");
            await Process.RunAsync("docker", $"compose -f {file} up -d {string.Join(" ", opt.With)}");
            AnsiConsole.MarkupLine("[green]Dev env is UP[/]");
            await Doctor.RunCheckList();
        })
    },
    new Command("down"),
    new Command("seed", "Send sample messages")
    {
        new Option<string>("--scenario"),
        new Option<int>("--count", () => 1000),
    },
    new Command("tail", "Tail a topic/queue") { new Option<string>("--queue") },
};
```

## 9. `avtobus bench` — бенчмарки

```csharp
var bench = new Command("bench", "Run benchmarks")
{
    new Option<string>("--transport", () => "inmemory"),
    new Option<string>("--connection-string"),
    new Option<int>("--size", () => 1024),
    new Option<int>("--duration", () => 30),
    new Option<int>("--writers", () => 1),
    new Option<int>("--readers", () => 1),
    new Option<string>("--scenario", () => "pubsub") { Description = "pubsub|reqreply|fanout" },
    Handler = CommandHandler.Create<BenchOptions>(BenchRunner.RunAsync),
};
```

Вывод — красивый отчёт Spectre.Console:

```
┌─ AvtoBus Benchmark ──────────────────────────────────────┐
│ Scenario: pubsub    Transport: rabbit   Payload: 1 KB    │
│ Duration: 30s       Writers: 2         Readers: 4        │
├───────────┬───────────┬───────────┬───────────┬───────────┤
│ msg/s     │ p50       │ p95       │ p99       │ alloc/msg │
├───────────┼───────────┼───────────┼───────────┼───────────┤
│ 112,403   │ 0.9 ms    │ 6.3 ms    │ 18.4 ms   │ 1.8 KB    │
└───────────┴───────────┴───────────┴───────────┴───────────┘
```

## 10. `avtobus completion` (shell autocomplete)

```csharp
var completion = new Command("completion", "Generate shell completion")
{
    new Argument<string>("shell") { Description = "zsh|bash|fish|powershell" },
    Handler = CommandHandler.Create<string>(shell =>
    {
        var script = shell switch
        {
            "zsh" => ShellCompletionScript.Zsh("avtobus"),
            "bash" => ShellCompletionScript.Bash("avtobus"),
            "fish" => ShellCompletionScript.Fish("avtobus"),
            "pwsh" => ShellCompletionScript.Powershell("avtobus"),
            _ => throw new ArgumentException($"Unknown shell {shell}")
        };
        Console.Write(script);
        return 0;
    })
};
```

После `avtobus completion zsh >> ~/.zshrc && source ~/.zshrc`:

```bash
avtobus dlq replay --queue orders.<TAB>
# перебирает существующие очереди из ~/.config/avtobus/connection.json
```

## 11. Конфиг CLI `~/.config/avtobus/config.json`

```json
{
  "defaultTransport": "rabbit",
  "connections": {
    "dev": {
      "transport": "rabbit",
      "connectionString": "amqp://guest:guest@localhost:5672"
    },
    "staging": {
      "transport": "kafka",
      "connectionString": "brokers=kafka-stg:9092"
    }
  },
  "defaults": {
    "dlqReplayRate": 10,
    "format": "table"
  }
}
```

Переключение контекстов:
```bash
avtobus config use staging
avtobus dlq list orders.error
```

## 12. Интерактивный режим `avtobus repl`

```csharp
var repl = new Command("repl", "Interactive REPL")
{
    Handler = CommandHandler.Create(async (IConsole console) =>
    {
        console.WriteLine("AvtoBus REPL — type 'help' for commands");
        using var bus = ConnectFromDefaultProfile();
        var parser = BuildParser(bus);
        while (true)
        {
            console.Write("avtobus> ");
            var line = Console.ReadLine();
            if (line is null or "exit" or "quit") break;
            try { await parser.InvokeAsync(line); }
            catch (Exception ex) { console.Error.WriteLine(ex.Message); }
        }
    })
};
```

```text
avtobus> publish PlaceOrder '{"orderId":"8e7d5f9a...","items":[]}'
Sent: 7d8e67b1-...
avtobus> peek orders --count 3
┌───┬─────────────┬──────────┬──────────────┐
│ # │ MessageType │ Id       │ Error        │
├───┼─────────────┼──────────┼──────────────┤
│ 1 │ PlaceOrder  │ 7d8e...  │ ValidationE. │
└───┴─────────────┴──────────┴──────────────┘
```

## 13. Выходные форматы

Все команды поддерживают `--format table|json|jsonl`:

- `table` — Spectre.Console для людей;
- `json` — один JSON-объект для скриптов;
- `jsonl` — one-object-per-line для `jq`/пайпов.

Пример:
```bash
avtobus dlq list orders.error --format json | jq '[.[] | select(.type == "PlaceOrder")]'
```
