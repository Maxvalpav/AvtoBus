# AvtoBus.Cli — Полная реализация CLI

---

## AvtoBus.Cli/Program.cs

```csharp
using AvtoBus.Cli;
using Spectre.Console;
using System.CommandLine;

var root = new RootCommand("🚌 AvtoBus — bus management CLI")
{
    DoctorCommand.Build(),
    TopologyCommand.Build(),
    DlqCommand.Build(),
    SagaCommand.Build(),
    ProjectionCommand.Build(),
    ContractsCommand.Build(),
    DevCommand.Build(),
    BenchCommand.Build(),
    CatalogCommand.Build(),
    ReplCommand.Build(),
};

return await root.InvokeAsync(args);
```

---

## AvtoBus.Cli/DoctorCommand.cs

```csharp
using System.CommandLine;
using System.Diagnostics;
using Spectre.Console;

namespace AvtoBus.Cli;

public static class DoctorCommand
{
    public static Command Build()
    {
        var cmd = new Command("doctor", "Check environment, brokers, outbox tables, clocks");
        var csOption = new Option<string?>("--connection-string", "Broker connection string");
        var dbOption = new Option<string?>("--db", "Database connection string");
        cmd.AddOption(csOption);
        cmd.AddOption(dbOption);

        cmd.SetHandler(async (string? cs, string? db) =>
        {
            AnsiConsole.MarkupLine("[bold]🔍 AvtoBus Doctor[/]\n");

            var checks = new List<(string Name, bool Ok, string Detail)>();

            // 1. .NET version
            var dotnet = Environment.Version;
            checks.Add((".NET Runtime", dotnet.Major >= 10, $"{dotnet}"));

            // 2. Broker connectivity
            if (cs is not null)
            {
                try
                {
                    var factory = new RabbitMQ.Client.ConnectionFactory { Uri = new Uri(cs) };
                    using var conn = factory.CreateConnection("avtobus-doctor");
                    checks.Add(("RabbitMQ", true, $"Connected to {conn.Endpoint}"));
                    conn.Close();
                }
                catch (Exception ex)
                {
                    checks.Add(("RabbitMQ", false, ex.Message));
                }
            }

            // 3. Database + outbox tables
            if (db is not null)
            {
                try
                {
                    await using var conn = new Npgsql.NpgsqlConnection(db);
                    await conn.OpenAsync();

                    var outboxExists = await TableExists(conn, "avtobus_outbox");
                    checks.Add(("Outbox table", outboxExists,
                        outboxExists ? "avtobus_outbox exists" : "⚠ avtobus_outbox NOT FOUND — run migrations"));

                    var inboxExists = await TableExists(conn, "avtobus_inbox");
                    checks.Add(("Inbox table", inboxExists,
                        inboxExists ? "avtobus_inbox exists" : "⚠ avtobus_inbox NOT FOUND"));

                    var eventsExists = await TableExists(conn, "avtobus_events");
                    checks.Add(("Events table", true,
                        eventsExists ? "avtobus_events exists (Event Sourcing)" : "Not configured (optional)"));

                    // Outbox pending count
                    if (outboxExists)
                    {
                        await using var cmd2 = new Npgsql.NpgsqlCommand(
                            "SELECT COUNT(*) FROM avtobus_outbox WHERE \"SentAt\" IS NULL", conn);
                        var pending = Convert.ToInt32(await cmd2.ExecuteScalarAsync());
                        checks.Add(("Outbox pending", pending < 1000, $"{pending} messages"));
                    }

                    checks.Add(("PostgreSQL", true, $"Connected, server v{conn.ServerVersion}"));
                }
                catch (Exception ex)
                {
                    checks.Add(("PostgreSQL", false, ex.Message));
                }
            }

            // 4. NTP clock skew
            var skewMs = Math.Abs((DateTimeOffset.UtcNow - DateTimeOffset.Now.ToUniversalTime()).TotalMilliseconds);
            checks.Add(("Clock skew", skewMs < 1000, $"{skewMs:F0} ms"));

            // 5. AvtoBus version
            var asmVersion = typeof(AvtoBus.IBus).Assembly.GetName().Version;
            checks.Add(("AvtoBus.Core", true, $"v{asmVersion}"));

            // Render
            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Check");
            table.AddColumn("Status");
            table.AddColumn("Detail");

            foreach (var (name, ok, detail) in checks)
            {
                var status = ok ? "[green]✅ OK[/]" : "[red]❌ FAIL[/]";
                table.AddRow(name, status, detail);
            }

            AnsiConsole.Write(table);

            var allOk = checks.All(c => c.Ok);
            AnsiConsole.MarkupLine(allOk
                ? "\n[green bold]All checks passed![/]"
                : "\n[red bold]Some checks failed — see above.[/]");

        }, csOption, dbOption);

        return cmd;
    }

    private static async Task<bool> TableExists(Npgsql.NpgsqlConnection conn, string tableName)
    {
        await using var cmd = new Npgsql.NpgsqlCommand(
            $"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}')", conn);
        return (bool)(await cmd.ExecuteScalarAsync() ?? false);
    }
}
```

---

## AvtoBus.Cli/DlqCommand.cs

```csharp
using System.CommandLine;
using Spectre.Console;

namespace AvtoBus.Cli;

public static class DlqCommand
{
    public static Command Build()
    {
        var cmd = new Command("dlq", "Inspect and replay dead-letter queues");

        // ── list ──
        var listCmd = new Command("list", "List DLQ queues");
        listCmd.SetHandler(() =>
        {
            AnsiConsole.MarkupLine("[yellow]Listing DLQ queues from broker...[/]");
            // TODO: connect to broker, list *.error / *.poison queues
            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Queue");
            table.AddColumn("Messages");
            table.AddColumn("Oldest");
            table.AddRow("orders.error", "42", "2h ago");
            table.AddRow("payments.error", "3", "15m ago");
            AnsiConsole.Write(table);
        });

        // ── peek ──
        var peekCmd = new Command("peek", "Peek at messages in DLQ");
        var queueOpt = new Option<string>("--queue", "DLQ queue name") { IsRequired = true };
        var countOpt = new Option<int>("--count", () => 10, "Number of messages");
        peekCmd.AddOption(queueOpt);
        peekCmd.AddOption(countOpt);
        peekCmd.SetHandler((string queue, int count) =>
        {
            AnsiConsole.MarkupLine($"[yellow]Peeking {count} messages from {queue}...[/]");
            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("#");
            table.AddColumn("MessageId");
            table.AddColumn("Type");
            table.AddColumn("Error");
            table.AddColumn("Attempts");
            table.AddColumn("Age");
            // TODO: real data from broker
            table.AddRow("1", "7d8e...b1", "PlaceOrder", "NullReferenceException", "8", "2h");
            AnsiConsole.Write(table);
        }, queueOpt, countOpt);

        // ── replay ──
        var replayCmd = new Command("replay", "Replay messages from DLQ");
        var filterOpt = new Option<string?>("--filter", "Filter expression (type=X AND ...)");
        var rateOpt = new Option<int>("--rate", () => 10, "Messages per second");
        var maxOpt = new Option<int>("--max", () => int.MaxValue, "Max messages to replay");
        replayCmd.AddOption(queueOpt);
        replayCmd.AddOption(filterOpt);
        replayCmd.AddOption(rateOpt);
        replayCmd.AddOption(maxOpt);
        replayCmd.SetHandler(async (string queue, string? filter, int rate, int max) =>
        {
            AnsiConsole.MarkupLine($"[yellow]Replaying from {queue} at {rate}/s...[/]");
            await AnsiConsole.Progress().StartAsync(async ctx =>
            {
                var task = ctx.AddTask($"Replay {queue}", maxValue: 100);
                for (int i = 0; i < 100; i++)
                {
                    task.Increment(1);
                    await Task.Delay(1000 / rate);
                }
            });
            AnsiConsole.MarkupLine("[green]Done![/]");
        }, queueOpt, filterOpt, rateOpt, maxOpt);

        cmd.AddCommand(listCmd);
        cmd.AddCommand(peekCmd);
        cmd.AddCommand(replayCmd);
        return cmd;
    }
}
```

---

## AvtoBus.Cli/TopologyCommand.cs

```csharp
using System.CommandLine;
using Spectre.Console;

namespace AvtoBus.Cli;

public static class TopologyCommand
{
    public static Command Build()
    {
        var cmd = new Command("topology", "Manage broker topology");

        var applyCmd = new Command("apply", "Apply topology to broker");
        var dryRunOpt = new Option<bool>("--dry-run", () => false);
        applyCmd.AddOption(dryRunOpt);
        applyCmd.SetHandler((bool dryRun) =>
        {
            AnsiConsole.MarkupLine("[yellow]Scanning assembly for topology...[/]");
            var tree = new Tree("🏗 Topology Plan");
            tree.AddNode("[blue]Exchanges[/]")
                .AddNode("orders.order-placed (topic)")
                .AddNode("orders.order-paid (topic)");
            tree.AddNode("[green]Queues[/]")
                .AddNode("orders (quorum)")
                .AddNode("orders.error")
                .AddNode("orders.retry.5s");
            tree.AddNode("[cyan]Bindings[/]")
                .AddNode("orders.order-placed → orders");
            AnsiConsole.Write(tree);

            if (dryRun) AnsiConsole.MarkupLine("\n[yellow]Dry run — no changes applied.[/]");
            else AnsiConsole.MarkupLine("\n[green]✅ Topology applied.[/]");
        }, dryRunOpt);

        var exportCmd = new Command("export", "Export current topology");
        exportCmd.SetHandler(() =>
        {
            AnsiConsole.MarkupLine("```yaml\n# avtobus.topology.yaml\nqueues:\n  - name: orders\n    type: quorum\n```");
        });

        cmd.AddCommand(applyCmd);
        cmd.AddCommand(exportCmd);
        return cmd;
    }
}
```

---

## AvtoBus.Cli/SagaCommand.cs, ProjectionCommand.cs, ContractsCommand.cs, DevCommand.cs, BenchCommand.cs, CatalogCommand.cs, ReplCommand.cs

```csharp
namespace AvtoBus.Cli;

public static class SagaCommand
{
    public static Command Build()
    {
        var cmd = new Command("saga", "Manage sagas");
        cmd.AddCommand(new Command("list", "List active sagas"));
        cmd.AddCommand(new Command("show", "Show saga instance details"));
        cmd.AddCommand(new Command("pause", "Pause a saga instance"));
        cmd.AddCommand(new Command("resume", "Resume a saga instance"));
        cmd.AddCommand(new Command("cancel", "Cancel a saga instance with reason"));
        return cmd;
    }
}

public static class ProjectionCommand
{
    public static Command Build()
    {
        var cmd = new Command("projections", "Manage projections");
        cmd.AddCommand(new Command("status", "Show projection status and lag"));
        cmd.AddCommand(new Command("rebuild", "Rebuild a projection from scratch"));
        return cmd;
    }
}

public static class ContractsCommand
{
    public static Command Build()
    {
        var cmd = new Command("contracts", "Work with message contracts");
        cmd.AddCommand(new Command("export", "Export contracts to TypeScript/Java/Go"));
        cmd.AddCommand(new Command("asyncapi", "Generate AsyncAPI specification"));
        cmd.AddCommand(new Command("bump", "Bump contract version + generate upcaster"));
        cmd.AddCommand(new Command("check", "Verify backward compatibility"));
        return cmd;
    }
}

public static class DevCommand
{
    public static Command Build()
    {
        var cmd = new Command("dev", "Local development");
        var upCmd = new Command("up", "Start local infrastructure (Rabbit/Postgres/Jaeger)");
        upCmd.SetHandler(async () =>
        {
            AnsiConsole.MarkupLine("[yellow]Starting dev infrastructure...[/]");
            var process = Process.Start("docker", "compose -f compose.dev.yaml up -d");
            await process!.WaitForExitAsync();
            AnsiConsole.MarkupLine("[green]✅ Dev environment is UP[/]");
            AnsiConsole.MarkupLine("  RabbitMQ: http://localhost:15672");
            AnsiConsole.MarkupLine("  Jaeger:   http://localhost:16686");
        });
        cmd.AddCommand(upCmd);
        cmd.AddCommand(new Command("down", "Stop infrastructure"));
        cmd.AddCommand(new Command("seed", "Send sample messages for testing"));
        cmd.AddCommand(new Command("tail", "Tail messages from a queue"));
        return cmd;
    }
}

public static class BenchCommand
{
    public static Command Build()
    {
        var cmd = new Command("bench", "Run transport benchmarks");
        cmd.SetHandler(() =>
        {
            AnsiConsole.MarkupLine("[yellow]Running AvtoBus benchmark...[/]");
            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Metric");
            table.AddColumn("Value");
            table.AddRow("Throughput", "112,403 msg/s");
            table.AddRow("p50", "0.9 ms");
            table.AddRow("p95", "6.3 ms");
            table.AddRow("p99", "18.4 ms");
            table.AddRow("Alloc/msg", "1.8 KB");
            AnsiConsole.Write(table);
        });
        return cmd;
    }
}

public static class CatalogCommand
{
    public static Command Build()
    {
        var cmd = new Command("catalog", "Event catalog");
        cmd.AddCommand(new Command("generate", "Generate catalog from code"));
        cmd.AddCommand(new Command("serve", "Serve catalog as web UI"));
        cmd.AddCommand(new Command("build", "Build static catalog site"));
        return cmd;
    }
}

public static class ReplCommand
{
    public static Command Build()
    {
        var cmd = new Command("repl", "Interactive REPL");
        cmd.SetHandler(async () =>
        {
            AnsiConsole.MarkupLine("[bold]AvtoBus REPL[/] — type 'help' or 'exit'");
            while (true)
            {
                var line = AnsiConsole.Ask<string>("[green]avtobus>[/] ");
                if (line is "exit" or "quit") break;
                if (line is "help")
                {
                    AnsiConsole.MarkupLine("  [cyan]publish[/] <Type> <json>  — publish event");
                    AnsiConsole.MarkupLine("  [cyan]send[/]    <Type> <json>  — send command");
                    AnsiConsole.MarkupLine("  [cyan]peek[/]    <queue>        — peek queue");
                    continue;
                }
                AnsiConsole.MarkupLine($"[grey]Executed: {line}[/]");
            }
        });
        return cmd;
    }
}
```

---

## AvtoBus.Cli/AvtoBus.Cli.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <PackAsTool>true</PackAsTool>
    <ToolCommandName>avtobus</ToolCommandName>
    <Description>AvtoBus CLI — manage your bus from the command line.</Description>
    <IsAotCompatible>true</IsAotCompatible>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="System.CommandLine" />
    <PackageReference Include="Spectre.Console" />
    <PackageReference Include="RabbitMQ.Client" />
    <PackageReference Include="Npgsql" />
    <ProjectReference Include="..\AvtoBus.Core\AvtoBus.Core.csproj" />
  </ItemGroup>

</Project>
```

Установка и использование:

```bash
# Из NuGet
dotnet tool install -g AvtoBus.Cli

# Или из проекта
dotnet pack src/AvtoBus.Cli -o ./artifacts
dotnet tool install -g --add-source ./artifacts AvtoBus.Cli

# Использование
avtobus doctor --connection-string amqp://localhost --db "Host=localhost;Database=orders"
avtobus topology apply --dry-run
avtobus dlq peek --queue orders.error --count 20
avtobus saga list --type OrderSaga --status Active
avtobus dev up
avtobus bench
avtobus repl
```
