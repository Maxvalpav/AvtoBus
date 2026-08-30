internal static class ErrorSanitizer
{
    public static string Message(string value) => Truncate(Redact(value), 2_000);
    public static string? Stack(string? value) => value is null ? null : Truncate(Redact(value), 16_000);

    private static string Redact(string value)
    {
        return value
            .Replace("Password=", "Password=[REDACTED]", StringComparison.OrdinalIgnoreCase)
            .Replace("Authorization: Bearer ", "Authorization: Bearer [REDACTED]", StringComparison.OrdinalIgnoreCase);
    }

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..max];
}
