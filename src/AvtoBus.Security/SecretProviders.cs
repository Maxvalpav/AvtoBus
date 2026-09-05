using Microsoft.Extensions.Configuration;

namespace AvtoBus.Security;

/// <summary>
/// Источник мастер-секрета «из коробки» (аудит 03 §3.1): секрет больше не обязан
/// жить строкой в коде/конфиг-лямбде — его отдаёт провайдер (конфигурация,
/// переменные окружения, файл <c>/run/secrets/…</c> для Docker/K8s).
/// Реализации синхронные и AOT-friendly: чтение config/env/файла —
/// быстро и не требует async. Будущим KMS-провайдерам — отдельный async-интерфейс.
/// </summary>
public interface ISecretProvider
{
    /// <summary>Секрет по имени или <c>null</c>, если провайдер его не знает.</summary>
    string? GetSecret(string name);
}

/// <summary>Секрет из <see cref="IConfiguration"/> (user-secrets, Key Vault provider, env).</summary>
public sealed class ConfigurationSecretProvider(IConfiguration configuration) : ISecretProvider
{
    public string? GetSecret(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return configuration[name];
    }
}

/// <summary>
/// Секрет из переменных окружения: имя маппится как <c>{Prefix}{name}</c>,
/// не-алфанумерики — в <c>_</c>, верхний регистр. Например, имя
/// <c>AvtoBus:MasterSecret</c> с префиксом <c>AVTOBUS_</c> ищется как
/// <c>AVTOBUS_AVTOBUS_MASTERSECRET</c>; удобнее передавать короткое имя
/// (<c>MASTERSECRET</c> → <c>AVTOBUS_MASTERSECRET</c>).
/// </summary>
public sealed class EnvironmentSecretProvider(string prefix = "AVTOBUS_") : ISecretProvider
{
    public string? GetSecret(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var key = prefix + string.Concat(name.Select(c => char.IsLetterOrDigit(c) ? char.ToUpperInvariant(c) : '_'));
        return Environment.GetEnvironmentVariable(key);
    }
}

/// <summary>
/// Секрет из файла: <c>{directory}/{name}</c> (Docker/K8s: <c>/run/secrets/avtobus-master-secret</c>).
/// Конечные переносы строк отрезаются (конвенция файл-маунтов). Читается лениво —
/// при каждом вызове, поэтому ротация секрета через пересоздание файла подхватывается
/// следующим чтением (KeyRing перечитает при следующей ротации, если провайдер задан).
/// </summary>
public sealed class FileSecretProvider(string directory = "/run/secrets") : ISecretProvider
{
    public string? GetSecret(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.IndexOfAny(['/', '\\', .. Path.GetInvalidFileNameChars()]) >= 0)
            return null;
        var path = Path.Combine(directory, name);
        if (!File.Exists(path))
            return null;
        return File.ReadAllText(path).TrimEnd('\r', '\n');
    }
}

/// <summary>Первый непустой секрет по цепочке провайдеров (config → env → файл).</summary>
public sealed class CompositeSecretProvider(params ISecretProvider[] providers) : ISecretProvider
{
    public string? GetSecret(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        foreach (var provider in providers)
        {
            if (provider.GetSecret(name) is { Length: > 0 } secret)
                return secret;
        }

        return null;
    }
}
