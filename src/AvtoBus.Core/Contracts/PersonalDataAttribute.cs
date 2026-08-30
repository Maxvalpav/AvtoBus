namespace AvtoBus.Contracts;

/// <summary>
/// Метка PII на контракте: поле (или весь контракт) содержит персональные данные (идея 456).
/// Одна аннотация питает все механики: маскирование в логах/дашборде, GDPR-отчёты,
/// шифрование в сторе. Когда метка стоит на самой записи — маскируется весь контракт.
/// </summary>
[AttributeUsage(
    AttributeTargets.Property | AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Parameter,
    AllowMultiple = false,
    Inherited = true)]
public sealed class PersonalDataAttribute : Attribute
{
    /// <summary>Произвольная категория PII, например <c>email</c>, <c>phone</c>, <c>ssn</c>.</summary>
    public string? Category { get; set; }
}
