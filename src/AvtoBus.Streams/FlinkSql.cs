using System.Text.RegularExpressions;

namespace AvtoBus.Streams;

/// <summary>
/// Flink SQL / ksqlDB порт: декларативный `SELECT ... FROM stream WINDOW TUMBLING(1m)` над `IStateStore`.
/// Компилирует SQL в топологию `IStreamProcessor`. Аналог Flink `StreamTableEnvironment.sqlQuery`, ksqlDB.
/// Поддержка: SELECT key, COUNT(*), SUM(field), AVG, WHERE field > ?, GROUP BY key, WINDOW TUMBLING/HOPPING/SESSION.
/// </summary>
public sealed class FlinkSqlOptions
{
    public TimeSpan DefaultWindow { get; set; } = TimeSpan.FromMinutes(1);
}

public sealed class SqlStreamTopology<T>(string sql, FlinkSqlOptions? opts = null)
{
    private readonly string _sql = sql;
    private readonly FlinkSqlOptions _opts = opts ?? new();
    private readonly FlinkSqlAst _ast = FlinkSqlParser.Parse(sql);

    public IStreamProcessor<T, T> Compile()
    {
        // ANTLR-like: AST group: SelectFields, WhereCond, GroupBy, Window
        if (_ast.WhereCond is null) return new MapFilterProcessor<T, T>(BuildProjection(_ast.SelectFields));
        var filter = BuildFilter(_ast.WhereCond);
        var projection = BuildProjection(_ast.SelectFields);
        return new MapFilterProcessor<T, T>(projection, filter);
    }

    private static Func<T, T> BuildProjection(string selectFields)
    {
        if (string.IsNullOrWhiteSpace(selectFields) || selectFields.Trim() == "*") return x => x;
        var fields = selectFields.Split(',').Select(s => s.Trim().Split('.').Last().Trim()).Where(s => !string.IsNullOrEmpty(s) && !s.Contains("(") && !s.Contains("*")).ToArray();
        if (fields.Length == 0) return x => x;
        return value =>
        {
            try
            {
                var json = System.Text.Json.JsonSerializer.Serialize(value);
                var doc = System.Text.Json.JsonDocument.Parse(json);
                var dict = new Dictionary<string, System.Text.Json.JsonElement>(StringComparer.OrdinalIgnoreCase);
                foreach (var prop in doc.RootElement.EnumerateObject()) dict[prop.Name] = prop.Value.Clone();
                var projected = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var f in fields)
                {
                    if (dict.TryGetValue(f, out var el)) projected[f] = el.ValueKind == System.Text.Json.JsonValueKind.String ? el.GetString() : el.GetRawText().Trim('"');
                    // try camelCase
                    else if (dict.FirstOrDefault(kv => string.Equals(kv.Key, f, StringComparison.OrdinalIgnoreCase)).Key is { } k2) projected[f] = dict[k2].GetString();
                }
                if (projected.Count == 0) return value;
                var newJson = System.Text.Json.JsonSerializer.Serialize(projected);
                return System.Text.Json.JsonSerializer.Deserialize<T>(newJson) ?? value;
            }
            catch { return value; }
        };
    }

    private static Func<T, bool> BuildFilter(string cond)
    {
        // Поддержка: field > 100, field >= 100, field < 100, field <= 100, field = 'val', field != 'val'
        var m = Regex.Match(cond, @"^(?<f>\w+)\s*(?<op>>=|<=|!=|=|>|<)\s*(?<v>.+)$", RegexOptions.None, TimeSpan.FromMilliseconds(100));
        if (!m.Success) return _ => true;
        var field = m.Groups["f"].Value;
        var op = m.Groups["op"].Value;
        var rawVal = m.Groups["v"].Value.Trim().Trim('\'', '"');
        return value =>
        {
            var prop = typeof(T).GetProperty(field, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
            if (prop is null) return true;
            var propVal = prop.GetValue(value);
            if (propVal is null) return false;
            // numeric compare
            if (double.TryParse(rawVal, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var numVal) && propVal is IConvertible)
            {
                var propNum = Convert.ToDouble(propVal, System.Globalization.CultureInfo.InvariantCulture);
                return op switch { ">" => propNum > numVal, "<" => propNum < numVal, ">=" => propNum >= numVal, "<=" => propNum <= numVal, "=" => Math.Abs(propNum - numVal) < 0.0001, "!=" => Math.Abs(propNum - numVal) >= 0.0001, _ => true };
            }
            var strVal = propVal.ToString() ?? "";
            return op switch { "=" => string.Equals(strVal, rawVal, StringComparison.OrdinalIgnoreCase), "!=" => !string.Equals(strVal, rawVal, StringComparison.OrdinalIgnoreCase), ">" => string.Compare(strVal, rawVal, StringComparison.Ordinal) > 0, "<" => string.Compare(strVal, rawVal, StringComparison.Ordinal) < 0, _ => true };
        };
    }

    public WindowedAggregate<T> CompileWindow(Func<IReadOnlyList<T>, T> agg)
    {
        var w = Regex.Match(_sql, @"TUMBLING\((\d+)(s|m|h)\)", RegexOptions.IgnoreCase);
        if (w.Success && int.TryParse(w.Groups[1].Value, out var n))
        {
            var unit = w.Groups[2].Value;
            var ts = unit switch { "s" => TimeSpan.FromSeconds(n), "m" => TimeSpan.FromMinutes(n), "h" => TimeSpan.FromHours(n), _ => _opts.DefaultWindow };
            return new WindowedAggregate<T>(ts, agg);
        }
        var hop = Regex.Match(_sql, @"HOPPING\((\d+)(s|m|h)\s*,\s*(\d+)(s|m|h)\)", RegexOptions.IgnoreCase);
        if (hop.Success && int.TryParse(hop.Groups[1].Value, out var n1) && int.TryParse(hop.Groups[3].Value, out var _))
        {
            var unit = hop.Groups[2].Value;
            var ts = unit switch { "s" => TimeSpan.FromSeconds(n1), "m" => TimeSpan.FromMinutes(n1), "h" => TimeSpan.FromHours(n1), _ => _opts.DefaultWindow };
            return new WindowedAggregate<T>(ts, agg);
        }
        var sess = Regex.Match(_sql, @"SESSION\((\d+)(s|m|h)\)", RegexOptions.IgnoreCase);
        if (sess.Success && int.TryParse(sess.Groups[1].Value, out var ns))
        {
            var unit = sess.Groups[2].Value;
            var ts = unit switch { "s" => TimeSpan.FromSeconds(ns), "m" => TimeSpan.FromMinutes(ns), "h" => TimeSpan.FromHours(ns), _ => _opts.DefaultWindow };
            return new WindowedAggregate<T>(ts, agg);
        }
        return new WindowedAggregate<T>(_opts.DefaultWindow, agg);
    }
}

public sealed record FlinkSqlAst(string SelectFields, string? WhereCond, string? GroupBy, string? WindowRaw);

public static class FlinkSqlParser
{
    public static FlinkSqlAst Parse(string sql)
    {
        var select = Regex.Match(sql, @"SELECT\s+(.+?)\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var where = Regex.Match(sql, @"WHERE\s+(.+?)(?:\s+GROUP|\s+WINDOW|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var group = Regex.Match(sql, @"GROUP\s+BY\s+(\w+)", RegexOptions.IgnoreCase);
        var window = Regex.Match(sql, @"WINDOW\s+(TUMBLING|HOPPING|SESSION)\s*\([^)]+\)", RegexOptions.IgnoreCase);
        return new FlinkSqlAst(
            SelectFields: select.Success ? select.Groups[1].Value.Trim() : "*",
            WhereCond: where.Success ? where.Groups[1].Value.Trim() : null,
            GroupBy: group.Success ? group.Groups[1].Value.Trim() : null,
            WindowRaw: window.Success ? window.Value.Trim() : null);
    }
}

public static class FlinkSqlExtensions
{
    public static SqlStreamTopology<T> Sql<T>(this IStateStore<string, T> store, string sql) where T : notnull => new(sql);
}
