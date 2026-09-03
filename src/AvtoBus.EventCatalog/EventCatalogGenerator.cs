using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using AvtoBus.AsyncApi;
using AvtoBus.Configuration;
using AvtoBus.Handlers;

namespace AvtoBus.EventCatalog;

/// <summary>Настройки каталога событий.</summary>
public sealed class EventCatalogOptions
{
    public string Title { get; set; } = "AvtoBus Event Catalog";
    public string Description { get; set; } = "";
    public string RoutePrefix { get; set; } = "catalog";
    /// <summary>Имя процесса-владельца: отображается в шапке каталога.</summary>
    public string? ServiceName { get; set; }
}

/// <summary>Владелец сообщения: класс-хендлер, который его обрабатывает.</summary>
public sealed record MessageOwner(string HandlerName, string HandlerType);

/// <summary>Запись каталога для одного сообщения.</summary>
public sealed record CatalogEntry(
    Type MessageType,
    string MessageName,
    bool IsCommand,
    string Channel,
    string DestinationKind,
    string SchemaJson,
    IReadOnlyList<MessageOwner> Owners);

/// <summary>
/// Строит статический Event Catalog из compile-time модели шины: дерево сообщений,
/// JSON-схемы, владельцы-хендлеры, маршруты. Генерирует самодостаточный HTML-сайт
/// (single-file, без внешних зависимостей) и/или чистый JSON для CI-диффа (идеи 137, 138).
/// </summary>
public sealed class EventCatalogGenerator
{
    private readonly DispatcherRegistry _dispatchers;
    private readonly RoutingTable _router;
    private readonly EventCatalogOptions _options;
    private readonly AsyncApiGenerator _asyncApi;

    public EventCatalogGenerator(
        DispatcherRegistry dispatchers,
        RoutingTable router,
        EventCatalogOptions options,
        AsyncApiGenerator asyncApi)
    {
        _dispatchers = dispatchers;
        _router = router;
        _options = options;
        _asyncApi = asyncApi;
    }

    /// <summary>Все записи каталога, отсортированные по имени сообщения.</summary>
    /// <remarks>Рефлексия по контрактам (см. <see cref="GenerateJson"/>): под строгим AOT избегайте.</remarks>
    public IReadOnlyList<CatalogEntry> Entries
        => _dispatchers.HandledTypes
            .Select(BuildEntry)
            .OrderBy(e => e.MessageName, StringComparer.Ordinal)
            .ToArray();

    /// <summary>Каталог в виде JSON: стабильный, для CI-диффа по PR (идея 138).</summary>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Каталог сканирует свойства контрактов через рефлексию — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Каталог сериализует схемы через reflection-STJ — несовместимо с NativeAOT.")]
    public string GenerateJson()
    {
        var doc = new
        {
            catalog = Entries.Select(e => new
            {
                message = e.MessageName,
                kind = e.IsCommand ? "command" : "event",
                channel = e.Channel,
                destination = e.DestinationKind,
                owners = e.Owners.Select(o => o.HandlerName).ToArray(),
                schema = JsonDocument.Parse(e.SchemaJson).RootElement,
            }),
            asyncapi = JsonDocument.Parse(_asyncApi.Generate()).RootElement,
        };

        return JsonSerializer.Serialize(doc, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        });
    }

    /// <summary>Самодостаточный HTML-сайт каталога (single-file).</summary>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Каталог сканирует свойства контрактов через рефлексию — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Каталог сериализует схемы через reflection-STJ — несовместимо с NativeAOT.")]
    public string GenerateHtml()
    {
        var entries = Entries;
        var rows = new StringBuilder();
        foreach (var entry in entries)
        {
            var kindLabel = entry.IsCommand ? "command" : "event";
            var kindClass = entry.IsCommand ? "tag command" : "tag event";
            var owners = entry.Owners.Count == 0
                ? "<span class=\"muted\">—</span>"
                : string.Join(", ", entry.Owners.Select(o => $"<code>{Html(o.HandlerName)}</code>"));

            rows.Append($"""
                <div class="card" id="{Html(entry.MessageName)}">
                  <div class="card-head">
                    <code class="msg">{Html(entry.MessageName)}</code>
                    <span class="{kindClass}">{kindLabel}</span>
                  </div>
                  <div class="meta">
                    <span>→ <code>{Html(entry.Channel)}</code> ({Html(entry.DestinationKind.ToLowerInvariant())})</span>
                  </div>
                  <div class="owners">owners: {owners}</div>
                  <pre class="schema">{Html(entry.SchemaJson)}</pre>
                </div>
                """);
        }

        var service = _options.ServiceName is null ? "" : $" — {Html(_options.ServiceName)}";
        var count = entries.Count;
        var summary = entries
            .GroupBy(e => e.IsCommand ? "command" : "event")
            .Select(g => $"{g.Count()} {g.Key}{(g.Count() == 1 ? "" : "s")}")
            .ToArray();

        return $$"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>{{Html(_options.Title)}}{{service}}</title>
          <style>
            :root { color-scheme: light dark; }
            body { font: 15px/1.5 system-ui, sans-serif; margin: 0; padding: 2rem; max-width: 1100px; margin-inline: auto; }
            h1 { margin-bottom: .25rem; }
            .muted, .meta { color: gray; }
            .summary { margin: .5rem 0 1.5rem; }
            .tag { font-size: 11px; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; letter-spacing: .05em; }
            .tag.command { background: #ffe8cc; color: #7a4b00; }
            .tag.event { background: #d6f5e3; color: #0a5c2e; }
            .card { border: 1px solid #8882; border-radius: 10px; padding: 1rem 1.25rem; margin-bottom: 1rem; }
            .card-head { display: flex; gap: .75rem; align-items: center; }
            .msg { font-size: 1.1rem; font-weight: 600; }
            .owners { margin-top: .5rem; }
            .schema { margin-top: .75rem; background: #0000000c; border-radius: 8px; padding: .75rem; overflow-x: auto; font-size: 12px; }
            pre.schema { white-space: pre-wrap; word-break: break-word; }
            code { font-family: ui-monospace, Consolas, monospace; }
          </style>
        </head>
        <body>
          <h1>{{Html(_options.Title)}}</h1>
          <div class="summary">{{count}} messages ({string.Join(", ", summary)})</div>
          {{rows}}
        </body>
        </html>
        """;
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Запись каталога строится из явно зарегистрированных контрактов; GenerateJson/GenerateHtml аннотированы RUC.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Запись каталога строится из явно зарегистрированных контрактов; GenerateJson/GenerateHtml аннотированы RDC.")]
    private CatalogEntry BuildEntry(Type type)
    {
        var isCommand = typeof(ICommand).IsAssignableFrom(type);
        var kind = isCommand ? OutgoingKind.Send : OutgoingKind.Publish;
        var route = _router.Resolve(type, kind);

        var owners = _dispatchers.For(type)
            .Select(d => new MessageOwner(d.HandlerName, d.GetType().FullName ?? ""))
            .ToArray();

        return new CatalogEntry(
            type,
            MessageTypeNaming.NameOf(type),
            isCommand,
            route.Destination.Name,
            route.Destination.Kind.ToString(),
            BuildSchemaJson(type),
            owners);
    }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Схема строится сканированием свойств контракта — только из Entries/GenerateJson.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Схема сериализуется через reflection-STJ — только из Entries/GenerateJson.")]
    private static string BuildSchemaJson(Type type)
    {
        var props = type.GetProperties()
            .Where(p => p.GetIndexParameters().Length == 0)
            .ToDictionary(
                p => p.Name,
                p => MapClrToJsonType(p.PropertyType) as object);

        return JsonSerializer.Serialize(
            new { type = "object", properties = props },
            new JsonSerializerOptions { WriteIndented = true });
    }

    private static string MapClrToJsonType(Type t)
    {
        if (t == typeof(string) || t == typeof(Guid) || t == typeof(char)
            || t == typeof(DateTime) || t == typeof(DateTimeOffset) || t == typeof(TimeSpan))
            return "string";
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return "integer";
        if (t == typeof(decimal) || t == typeof(double) || t == typeof(float))
            return "number";
        if (t == typeof(bool))
            return "boolean";
        if (t.IsEnum)
            return "string";
        return "object";
    }

    private static string Html(string s) => HtmlEncoder.Default.Encode(s);
}
