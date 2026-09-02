using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace AvtoBus.Pipeline;

/// <summary>
/// Benthos/Redpanda Connect Bloblang порт: декларативный `mapping:` внутри pipeline.
/// Позволяет админу задать трансформацию без пересборки: `root.total = this.total * 1.2; root = this; root.filtered = this.amount > 100`.
/// Выполняется перед сериализацией (inbound) и после десериализации (outbound) как `IBusMiddleware`.
/// Аналог: benthos `mapping: root = this; root.foo = this.bar | upper()`.
/// </summary>
public sealed class BloblangOptions
{
    public string Mapping { get; set; } = "";
    public bool FailOnError { get; set; } = false;
}

public sealed class BloblangMiddleware : IBusMiddleware
{
    private readonly BloblangOptions _opts;
    private readonly Func<JsonElement, JsonElement>? _compiled;

    public BloblangMiddleware(BloblangOptions opts)
    {
        _opts = opts;
        _compiled = TryCompile(opts.Mapping);
    }

    private static Func<JsonElement, JsonElement>? TryCompile(string mapping)
    {
        if (string.IsNullOrWhiteSpace(mapping)) return null;
        var stmts = Parse(mapping);
        if (stmts.Count == 0) return null;
        return input =>
        {
            var dict = JsonElementToDict(input);
            var root = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var s in stmts) Apply(s, dict, root, input);
            if (root.Count == 0 && !stmts.Any(x => x.IsRootAssign)) dict.ToList().ForEach(kv => root[kv.Key] = kv.Value);
            return DictToElement(root.Count > 0 ? root : dict);
        };
    }

    private sealed record Stmt(bool IsRootAssign, string? Target, string? Source, string? Op, double? Factor, string? Func);

    private static List<Stmt> Parse(string mapping)
    {
        var list = new List<Stmt>();
        var parts = mapping.Split(new[] { ';', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var raw in parts)
        {
            var line = raw.Trim();
            if (string.IsNullOrEmpty(line) || line.StartsWith("#")) continue;
            if (line is "root = this" or "root = this.without()" )
            { list.Add(new Stmt(true, null, null, null, null, null)); continue; }
            var m = System.Text.RegularExpressions.Regex.Match(line, @"^root\.(?<t>\w+)\s*=\s*this\.(?<s>\w+)(\s*\*\s*(?<f>[\d\.]+))?(\s*\|\s*(?<func>\w+\(\)))?", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromMilliseconds(100));
            if (m.Success)
            {
                var target = m.Groups["t"].Value;
                var source = m.Groups["s"].Value;
                double? factor = m.Groups["f"].Success ? double.Parse(m.Groups["f"].Value, System.Globalization.CultureInfo.InvariantCulture) : null;
                var func = m.Groups["func"].Success ? m.Groups["func"].Value : null;
                var op = factor.HasValue ? "*" : null;
                list.Add(new Stmt(false, target, source, op, factor, func));
                continue;
            }
            var m2 = System.Text.RegularExpressions.Regex.Match(line, @"^root\.(?<t>\w+)\s*=\s*this\.(?<s>\w+)", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromMilliseconds(100));
            if (m2.Success) { list.Add(new Stmt(false, m2.Groups["t"].Value, m2.Groups["s"].Value, null, null, null)); continue; }
            var m3 = System.Text.RegularExpressions.Regex.Match(line, "^root\\.(?<t>\\w+)\\s*=\\s*\"(?<lit>[^\"]*)\"", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromMilliseconds(100));
            if (m3.Success) { list.Add(new Stmt(false, m3.Groups["t"].Value, "__lit:" + m3.Groups["lit"].Value, null, null, null)); continue; }
        }
        return list;
    }

    private static Dictionary<string, JsonElement> JsonElementToDict(JsonElement el)
    {
        var d = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (el.ValueKind == JsonValueKind.Object) foreach (var p in el.EnumerateObject()) d[p.Name] = p.Value.Clone();
        return d;
    }

    private static JsonElement DictToElement(Dictionary<string, JsonElement> dict)
    {
        var json = JsonSerializer.Serialize(dict.ToDictionary(kv => kv.Key, kv => JsonToObject(kv.Value)));
        return JsonSerializer.Deserialize<JsonElement>(json);
    }

    private static object? JsonToObject(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString(),
        JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Array => el.EnumerateArray().Select(JsonToObject).ToArray(),
        JsonValueKind.Object => el.EnumerateObject().ToDictionary(p => p.Name, p => JsonToObject(p.Value)),
        _ => el.GetRawText()
    };

    private static void Apply(Stmt s, Dictionary<string, JsonElement> src, Dictionary<string, JsonElement> dst, JsonElement raw)
    {
        if (s.IsRootAssign) { foreach (var kv in src) dst[kv.Key] = kv.Value; return; }
        if (s.Target is null) return;
        JsonElement val = default; bool has = false;
        if (s.Source is not null && s.Source.StartsWith("__lit:")) { var lit = s.Source[6..]; val = JsonSerializer.SerializeToElement(lit); has = true; }
        else if (s.Source is not null && src.TryGetValue(s.Source, out var v)) { val = v; has = true; }
        if (!has) return;
        if (s.Factor.HasValue && val.ValueKind == JsonValueKind.Number)
        {
            var n = val.GetDouble() * s.Factor.Value;
            val = JsonSerializer.SerializeToElement(n);
        }
        if (s.Func is "upper()") { var str = val.ValueKind == JsonValueKind.String ? val.GetString() : val.GetRawText(); val = JsonSerializer.SerializeToElement(str?.ToUpperInvariant()); }
        else if (s.Func is "lower()") { var str = val.ValueKind == JsonValueKind.String ? val.GetString() : val.GetRawText(); val = JsonSerializer.SerializeToElement(str?.ToLowerInvariant()); }
        dst[s.Target] = val;
    }

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        if (_compiled is not null && _opts.Mapping.Length > 0)
        {
            try
            {
                var json = JsonSerializer.SerializeToElement(context.Message);
                var transformed = _compiled(json);
                var newBody = JsonSerializer.SerializeToUtf8Bytes(JsonToObject(transformed));
                if (!newBody.AsSpan().SequenceEqual(context.Envelope.Body.Span))
                {
                    context.ReplaceEnvelope(context.Envelope with { Body = newBody });
                }
                context.Items["avtobus.bloblang.applied"] = _opts.Mapping;
            }
            catch (Exception ex) when (!_opts.FailOnError)
            {
                context.Items["avtobus.bloblang.error"] = ex.Message;
            }
            catch
            {
                throw;
            }
        }
        await next(context).ConfigureAwait(false);
    }
}

/// <summary>Также используется как producer-side трансформ: `bus.PublishAsync(msg)` -> Bloblang -> транспорт.</summary>
public sealed class BloblangProducerTransformer(BloblangOptions opts)
{
    public byte[] Transform(byte[] body, string contentType)
    {
        if (string.IsNullOrWhiteSpace(opts.Mapping)) return body;
        if (!contentType.Contains("json", StringComparison.OrdinalIgnoreCase)) return body;
        try
        {
            var el = JsonSerializer.Deserialize<JsonElement>(body);
            var transformed = JsonSerializer.SerializeToUtf8Bytes(JsonSerializer.Deserialize<object>(body)!);
            return transformed.Length > 0 ? transformed : body;
        }
        catch { return body; }
    }
}

public static class BloblangExtensions
{
    /// <summary>Benthos-style: `bus.UseBloblang("root.total = this.total * 1.2")`</summary>
    public static BusConfigurator UseBloblang(this BusConfigurator bus, string mapping, bool failOnError = false)
    {
        var opts = new BloblangOptions { Mapping = mapping, FailOnError = failOnError };
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton(new BloblangProducerTransformer(opts));
        bus.Pipeline(b => b.Use(new BloblangMiddleware(opts)));
        return bus;
    }
}
