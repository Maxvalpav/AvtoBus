using System.Security.Claims;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// Верифицирующий экстрактор principal (идея 454, fail-closed): заголовку
/// <c>avtobus-user</c> доверяет только при валидной HMAC-подписи конверта.
/// Без подписи (подделка, RequireSignature выключен, старый отправитель) возвращает
/// null — хендлеры с <c>[BusAuthorize]</c> отклонят сообщение, а не примут чужую роль.
/// Подставляет себя вместо <c>HeaderPrincipalExtractor</c> при подключении безопасности.
/// </summary>
public sealed class SignedPrincipalExtractor(EnvelopeSecurity security) : IPrincipalExtractor
{
    public ClaimsPrincipal? Extract(Envelope envelope)
    {
        var wire = envelope.Header(BusHeaders.User);
        if (string.IsNullOrEmpty(wire))
            return null;
        if (!security.HasValidSignature(envelope))
            return null;
        return PrincipalSerializer.Deserialize(wire);
    }
}
