using System.Security.Claims;
using System.Text;
using System.Text.Json;
using AvtoBus.Serialization;

namespace AvtoBus;

/// <summary>
/// Сериализация <see cref="ClaimsPrincipal"/> в заголовок конверта и обратно (идея 454).
/// Формат — компактный JSON: имя, режим аутентификации и список claims. Достаточно для
/// восстановления ролей и имени пользователя на принимающей стороне; подпись добавляет
/// подключённая безопасность (AvtoBus.Security).
/// </summary>
public static class PrincipalSerializer
{
    public static string? Serialize(ClaimsPrincipal? principal)
    {
        if (principal is null)
            return null;

        var wire = new UserWire
        {
            Name = principal.Identity?.Name,
            AuthenticationType = principal.Identity?.AuthenticationType,
            Claims = principal.Claims
                .Select(c => new ClaimWire(c.Type, c.Value, c.ValueType, c.Issuer))
                .ToArray(),
        };

        // Source-generated контекст: без рефлексии (AOT-safe); wire-формат не меняется.
        return Convert.ToBase64String(
            JsonSerializer.SerializeToUtf8Bytes(wire, AvtoBusCoreJsonContext.Default.UserWire));
    }

    public static ClaimsPrincipal? Deserialize(string? wire)
    {
        if (string.IsNullOrEmpty(wire))
            return null;

        try
        {
            var decoded = (UserWire?)JsonSerializer.Deserialize(
                Convert.FromBase64String(wire),
                AvtoBusCoreJsonContext.Default.UserWire);
            if (decoded is null)
                return null;

            var claims = (decoded.Claims ?? [])
                .Select(c => new Claim(c.Type, c.Value, c.ValueType ?? ClaimValueTypes.String, c.Issuer));

            if (string.IsNullOrEmpty(decoded.AuthenticationType))
                return new ClaimsPrincipal(new ClaimsIdentity(claims));

            return new ClaimsPrincipal(new ClaimsIdentity(claims, decoded.AuthenticationType, ClaimTypes.Name, ClaimTypes.Role));
        }
        catch (JsonException)
        {
            return null;
        }
        catch (FormatException)
        {
            return null;
        }
    }
}

internal sealed record UserWire
{
    public string? Name { get; init; }
    public string? AuthenticationType { get; init; }
    public ClaimWire[]? Claims { get; init; }
}

internal sealed record ClaimWire(string Type, string Value, string? ValueType, string? Issuer);
