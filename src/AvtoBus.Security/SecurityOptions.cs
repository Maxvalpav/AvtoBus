using System.Security.Cryptography;

namespace AvtoBus.Security;

/// <summary>
/// Настройки подсистемы безопасности конвертов (идеи 451, 455).
/// </summary>
public sealed class SecurityOptions
{
    /// <summary>Обязательная проверка подписи для входящих сообщений.</summary>
    public bool RequireSignature { get; set; }

    /// <summary>Шифровать тело исходящих сообщений (AES-GCM) и открывать входящие.</summary>
    public bool EncryptBody { get; set; }

    /// <summary>
    /// Раскручиваемый мастер-секрет: passphrase + соль дают ключ подписи и ключ шифрования.
    /// В проде ключи должны приходить из Key Vault / K8s secrets и ротироваться (идея 452) —
    /// здесь это простой, детерминированный способ для тестов и локальной разработки.
    /// </summary>
    public string MasterSecret { get; set; } = "";

    /// <summary>Rfc2898 итераций KDF — настраивается для тестов (медленно при больших).</summary>
    public int KdfIterations { get; set; } = 100_000;

    /// <summary>Имя service identity, которым подпишет исходящие сообщения этот сервис.</summary>
    public string SigningIdentity { get; set; } = "avtobus";

    /// <summary>Rate limit на отправку: макс. исходящих сообщений в секунду (идея 459). 0 — безлимит.</summary>
    public int OutboundRatePerSecond { get; set; }

    /// <summary>Интервал автоматической ротации ключей подписи/шифрования (идея 452). null — без ротации.</summary>
    public TimeSpan? KeyRotationInterval { get; set; }

    /// <summary>Когда ротация включена, сколько поколений старых ключей остаются валидными при проверке входящих.</summary>
    public int KeepPreviousKeyGenerations { get; set; } = 1;

    internal SecurityKeys Keys { get; set; } = SecurityKeys.Empty;

    public void UseGeneratedKeys() => Keys = SecurityKeys.Random();

    public void UseKeys(SecurityKeys keys) => Keys = keys;
}

/// <summary>Специальные ключи, замороженные в конфигурации (для тестов и мульти-инстанс отладки).</summary>
public sealed class SecurityKeys
{
    public required byte[] SigningKey { get; init; }
    public required byte[] EncryptionKey { get; init; }

    public static readonly SecurityKeys Empty =
        new() { SigningKey = [], EncryptionKey = [] };

    public static SecurityKeys Random()
    {
        var signing = RandomNumberGenerator.GetBytes(32);
        var encryption = RandomNumberGenerator.GetBytes(32);
        return new SecurityKeys { SigningKey = signing, EncryptionKey = encryption };
    }

    /// <summary>Детерминированные ключи из passphrase (PBKDF2) — обе стороны используют одни и те же.</summary>
    public static SecurityKeys FromSecret(string secret, int iterations = 100_000)
    {
        byte[] Derive(string salt) => Rfc2898DeriveBytes.Pbkdf2(
            secret,
            System.Text.Encoding.UTF8.GetBytes(salt),
            iterations,
            HashAlgorithmName.SHA256,
            32);

        return new SecurityKeys
        {
            SigningKey = Derive("signing"),
            EncryptionKey = Derive("encryption"),
        };
    }
}
