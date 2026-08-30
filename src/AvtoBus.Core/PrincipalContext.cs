using System.Security.Claims;

namespace AvtoBus;

/// <summary>
/// Текущий пользователь процесса отправки (идея 454): AsyncLocal-контекст, которым приложение
/// помечает сообщение при отправке. Приложение ставит его из HttpContext (аналог
/// <see cref="InitiatorContext"/>), EnvelopeFactory сериализует его в подписанный заголовок конверта.
/// </summary>
public static class PrincipalContext
{
    private static readonly AsyncLocal<ClaimsPrincipal?> Current = new();

    /// <summary>Устанавливает principal, от имени которого отправляются сообщения.</summary>
    public static IDisposable Push(ClaimsPrincipal? principal)
    {
        var previous = Current.Value;
        Current.Value = principal;
        return new PopOnDispose(previous);
    }

    public static ClaimsPrincipal? Get() => Current.Value;

    private sealed class PopOnDispose(ClaimsPrincipal? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            Current.Value = previous;
        }
    }
}

/// <summary>
/// Извлекает <see cref="ClaimsPrincipal"/> из входящего конверта (идея 454).
/// Реализация по умолчанию читает заголовок <c>avtobus-user</c> без проверки подписи —
/// для внутренних сред; подключённая безопасность (AvtoBus.Security) заменяет её
/// на верификацию подписанного контекста.
/// </summary>
public interface IPrincipalExtractor
{
    ClaimsPrincipal? Extract(Envelope envelope);
}

/// <summary>
/// Извлекает principal из заголовка <c>avtobus-user</c> без проверки подписи.
/// Используется по умолчанию; безопасность шины (AvtoBus.Security) подставляет верифицирующий.
/// </summary>
public sealed class HeaderPrincipalExtractor : IPrincipalExtractor
{
    public ClaimsPrincipal? Extract(Envelope envelope) => PrincipalSerializer.Deserialize(envelope.Header(BusHeaders.User));
}
