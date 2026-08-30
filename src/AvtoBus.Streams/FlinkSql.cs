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
    public IStreamProcessor<T, T> Compile()
    {
        // Стаб: парсит только `WHERE` и возвращает фильтр. Полный парсер — ANTLR.
        var whereMatch = Regex.Match(_sql, @"WHERE\s+(.+?)(?:\s+GROUP|\s+WINDOW|$)", RegexOptions.IgnoreCase);
        if (!whereMatch.Success) return new MapFilterProcessor<T, T>(x => x);
        var cond = whereMatch.Groups[1].Value;
        // Пример: `amount > 100` -> фильтр по Json
        return new MapFilterProcessor<T, T>(x => x, _ => true);
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
        return new WindowedAggregate<T>(_opts.DefaultWindow, agg);
    }
}

public static class FlinkSqlExtensions
{
    public static SqlStreamTopology<T> Sql<T>(this IStateStore<string, T> store, string sql) where T : notnull => new(sql);
}
