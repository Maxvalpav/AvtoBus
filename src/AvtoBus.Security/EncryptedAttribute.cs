namespace AvtoBus.Security;

/// <summary>
/// Поле помечено как зашифрованное (идея 455 per-field): значение уезжает в брокер как
/// <c>enc:&lt;base64 nonce&gt;:&lt;base64 ciphertext+tag&gt;</c>, на приёме расшифровывается.
/// Работает для string-полей контрактов; для других типов — сериализуйте в string.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class EncryptedAttribute : Attribute;
