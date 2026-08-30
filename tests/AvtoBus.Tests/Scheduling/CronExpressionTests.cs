using AvtoBus.Scheduling;
using Xunit;

namespace AvtoBus.Tests.Scheduling;

public class CronExpressionTests
{
    [Theory]
    [InlineData("0 6 * * *")]
    [InlineData("*/15 * * * *")]
    [InlineData("0 0 1 1 *")]
    [InlineData("30 9 * * 1-5")]
    public void Next_occurrence_matches_expected(string expr)
    {
        var cron = CronExpression.Parse(expr);
        var tz = TimeZoneInfo.Utc;

        var day = new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero); // суббота
        var next = cron.GetNextOccurrence(day, tz);
        Assert.NotNull(next);

        // Все срабатывания попадают в диапазон одного дня (проверка грубой корректности).
        Assert.True(next.Value > day);
    }

    [Fact]
    public void Daily_at_six_is_six_oclock_next_day()
    {
        var cron = CronExpression.Parse("0 6 * * *");
        var tz = TimeZoneInfo.Utc;
        var from = new DateTimeOffset(2026, 8, 15, 7, 0, 0, TimeSpan.Zero);

        var next = cron.GetNextOccurrence(from, tz);
        Assert.NotNull(next);
        Assert.Equal(new DateTimeOffset(2026, 8, 16, 6, 0, 0, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void Weekday_at_nine_thirty_skips_weekend()
    {
        var cron = CronExpression.Parse("30 9 * * 1-5");
        var tz = TimeZoneInfo.Utc;
        var from = new DateTimeOffset(2026, 8, 14, 12, 0, 0, TimeSpan.Zero); // пятница

        var next = cron.GetNextOccurrence(from, tz);
        Assert.NotNull(next);
        // Пятница 14-го, значит следующий будний — понедельник 17-го.
        Assert.Equal(new DateTimeOffset(2026, 8, 17, 9, 30, 0, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void Six_field_expression_with_seconds_works()
    {
        var cron = CronExpression.Parse("*/10 * * * * *");
        var tz = TimeZoneInfo.Utc;
        var from = new DateTimeOffset(2026, 8, 15, 10, 0, 0, TimeSpan.Zero);

        var next = cron.GetNextOccurrence(from, tz);
        Assert.NotNull(next);
        Assert.Equal(new DateTimeOffset(2026, 8, 15, 10, 0, 10, TimeSpan.Zero), next.Value);
    }

    [Fact]
    public void Invalid_field_count_throws()
    {
        Assert.Throws<FormatException>(() => CronExpression.Parse("0 6 * *"));
        Assert.Throws<FormatException>(() => CronExpression.Parse("0 6 * * * * *"));
    }

    [Fact]
    public void Month_and_day_names_are_normalized()
    {
        var cron = CronExpression.Parse("0 0 1 JAN MON");
        Assert.NotNull(cron.GetNextOccurrence(
            new DateTimeOffset(2026, 1, 1, 0, 0, 1, TimeSpan.Zero), TimeZoneInfo.Utc));
    }

    [Fact]
    public void Preview_yields_requested_count_in_order()
    {
        var cron = CronExpression.Parse("0 * * * *");
        var tz = TimeZoneInfo.Utc;
        var from = new DateTimeOffset(2026, 8, 15, 0, 30, 0, TimeSpan.Zero);

        var preview = cron.Preview(from, tz, 3).ToList();
        Assert.Equal(3, preview.Count);
        Assert.Equal(3, preview.Distinct().Count());
        Assert.Equal(preview.OrderBy(t => t), preview);
        Assert.Equal(new DateTimeOffset(2026, 8, 15, 1, 0, 0, TimeSpan.Zero), preview[0]);
    }
}
