using System.Text;
using AvtoBus.Dashboard;

namespace AvtoBus.DashboardTests;

/// <summary>Регрессия: просмотр DLQ санитизируется (PII-маски, redact заголовков).</summary>
public sealed class DashboardSanitizeTests
{
    [Fact]
    public void MaskPiiFields_masks_high_signal_fields_and_keeps_rest()
    {
        var body = Encoding.UTF8.GetBytes(
            """{"email":"a@b.c","phone":"+7000","orderId":"42","total":5}""");

        var masked = Encoding.UTF8.GetString(DashboardService.MaskPiiFields(body).Span);

        Assert.DoesNotContain("a@b.c", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("+7000", masked, StringComparison.Ordinal);
        Assert.Contains("42", masked, StringComparison.Ordinal);
        Assert.Contains("###", masked, StringComparison.Ordinal);
    }

    [Fact]
    public void MaskPiiFields_leaves_non_json_untouched()
    {
        var body = new byte[] { 1, 2, 3, 255, 254 };
        Assert.Equal(body, DashboardService.MaskPiiFields(body).ToArray());
    }

    [Fact]
    public void Browse_sanitization_is_on_and_tenant_filter_off_by_default()
    {
        var options = new DashboardOptions();
        Assert.True(options.SanitizeBrowse);
        Assert.Null(options.TenantId);
        Assert.Contains("avtobus-user", options.RedactedHeaders);
        Assert.Contains("avtobus-exception-stack", options.RedactedHeaders);
    }
}
