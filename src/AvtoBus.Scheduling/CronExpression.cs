namespace AvtoBus.Scheduling;

/// <summary>
/// Парсер cron-выражений (5 или 6 полей: [sec] min hour day month dow).
/// Поддержка: <c>*</c>, <c>,</c>, <c>-</c>, <c>/</c> и имена месяцев/дней (идея 223).
/// </summary>
public sealed class CronExpression
{
    private bool[] _seconds = new bool[60];
    private bool[] _minutes = new bool[60];
    private bool[] _hours = new bool[24];
    private bool[] _daysOfMonth = new bool[31];
    private bool[] _months = new bool[12];
    private bool[] _daysOfWeek = new bool[7];
    private readonly bool _domAlways;
    private readonly bool _dowAlways;

    public string Expression { get; }

    private CronExpression(string expression, bool domAlways, bool dowAlways)
    {
        Expression = expression;
        _domAlways = domAlways;
        _dowAlways = dowAlways;
    }

    public static CronExpression Parse(string expression)
    {
        var parts = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length is not (5 or 6))
            throw new FormatException($"Cron must have 5 or 6 fields, got {parts.Length}: '{expression}'");

        var offset = parts.Length == 6 ? 1 : 0;

        var seconds = new bool[60];
        var minutes = new bool[60];
        var hours = new bool[24];
        var daysOfMonth = new bool[31];
        var months = new bool[12];
        var daysOfWeek = new bool[7];

        if (offset == 1)
            ParseField(parts[0], seconds, 0, 59);
        else
            seconds[0] = true;

        ParseField(parts[offset + 0], minutes, 0, 59);
        ParseField(parts[offset + 1], hours, 0, 23);
        ParseField(parts[offset + 2], daysOfMonth, 1, 31);
        ParseField(NormalizeMonths(parts[offset + 3]), months, 1, 12);
        ParseField(NormalizeDows(parts[offset + 4]), daysOfWeek, 0, 6);

        var domAlways = IsAlways(daysOfMonth, 1, 31);
        var dowAlways = IsAlways(daysOfWeek, 0, 6);

        return new CronExpression(expression, domAlways, dowAlways)
        {
            _seconds = seconds,
            _minutes = minutes,
            _hours = hours,
            _daysOfMonth = daysOfMonth,
            _months = months,
            _daysOfWeek = daysOfWeek,
        };
    }

    private static bool[] ParseField(string field, bool[] target, int min, int max)
    {
        foreach (var part in field.Split(','))
        {
            var step = 1;
            var range = part;

            if (part.Contains('/'))
            {
                var split = part.Split('/');
                range = split[0];
                step = int.Parse(split[1]);
            }

            int from, to;
            if (range == "*")
                (from, to) = (min, max);
            else if (range.Contains('-'))
            {
                var split = range.Split('-');
                (from, to) = (int.Parse(split[0]), int.Parse(split[1]));
            }
            else
                from = to = int.Parse(range);

            for (var i = from; i <= to; i += step)
                if (i >= min && i <= max)
                    target[i - min] = true;
        }

        return target;
    }

    private static bool IsAlways(bool[] target, int min, int max)
    {
        if (target.Length != max - min + 1)
            return true;
        for (var i = 0; i < target.Length; i++)
            if (!target[i]) return false;
        return true;
    }

    private static string NormalizeMonths(string f) => f.ToUpperInvariant()
        .Replace("JAN", "1").Replace("FEB", "2").Replace("MAR", "3").Replace("APR", "4")
        .Replace("MAY", "5").Replace("JUN", "6").Replace("JUL", "7").Replace("AUG", "8")
        .Replace("SEP", "9").Replace("OCT", "10").Replace("NOV", "11").Replace("DEC", "12");

    private static string NormalizeDows(string f) => f.ToUpperInvariant()
        .Replace("SUN", "0").Replace("MON", "1").Replace("TUE", "2").Replace("WED", "3")
        .Replace("THU", "4").Replace("FRI", "5").Replace("SAT", "6").Replace("7", "0");

    /// <summary>
    /// Следующее срабатывание после указанного момента (в заданной TZ).
    /// </summary>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset after, TimeZoneInfo tz)
    {
        var local = TimeZoneInfo.ConvertTime(after, tz);

        // Стартуем с целой секунды; если задана секундная точность — с неё.
        var cursor = local.DateTime.AddSeconds(1);

        var limit = local.DateTime.AddYears(2);

        while (cursor < limit)
        {
            if (!_months[cursor.Month - 1]) { cursor = new DateTime(cursor.Year, cursor.Month, 1).AddMonths(1); continue; }
            if (!MatchesDay(cursor)) { cursor = cursor.Date.AddDays(1); continue; }
            if (!_hours[cursor.Hour]) { cursor = new DateTime(cursor.Year, cursor.Month, cursor.Day, cursor.Hour, 0, 0).AddHours(1); continue; }
            if (!_minutes[cursor.Minute]) { cursor = new DateTime(cursor.Year, cursor.Month, cursor.Day, cursor.Hour, cursor.Minute, 0).AddMinutes(1); continue; }
            if (!_seconds[cursor.Second]) { cursor = cursor.AddSeconds(1); continue; }

            return new DateTimeOffset(
                DateTime.SpecifyKind(cursor, DateTimeKind.Unspecified),
                tz.GetUtcOffset(cursor)).ToUniversalTime();
        }

        return null;
    }

    /// <summary>Стандартное cron-правило: если оба поля заданы конкретно — «ИЛИ», иначе действует конкретное.</summary>
    private bool MatchesDay(DateTime cursor)
    {
        if (_domAlways) return _daysOfWeek[(int)cursor.DayOfWeek];
        if (_dowAlways) return _daysOfMonth[cursor.Day - 1];
        return _daysOfMonth[cursor.Day - 1] || _daysOfWeek[(int)cursor.DayOfWeek];
    }

    /// <summary>Предпросмотр следующих N срабатываний (для дашборда, идея 223).</summary>
    public IEnumerable<DateTimeOffset> Preview(DateTimeOffset from, TimeZoneInfo tz, int count)
    {
        var current = from;
        for (var i = 0; i < count; i++)
        {
            var next = GetNextOccurrence(current, tz);
            if (next is null) yield break;
            yield return next.Value;
            current = next.Value;
        }
    }
}
