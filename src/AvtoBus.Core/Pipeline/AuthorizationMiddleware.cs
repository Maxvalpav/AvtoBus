using System.Collections.Concurrent;
using System.Security.Claims;
using AvtoBus.Handlers;

namespace AvtoBus.Pipeline;

/// <summary>
/// Проверяет требование <see cref="BusAuthorizeAttribute"/> против текущего principal.
/// Реализация в AvtoBus.Security может добавить политики поверх ролей (идея 453).
/// </summary>
public interface IAuthorizer
{
    ValueTask<bool> AuthorizeAsync(
        ClaimsPrincipal? principal,
        BusAuthorizeAttribute requirement,
        ConsumeContext context);
}

/// <summary>
/// По умолчанию: роли из <see cref="BusAuthorizeAttribute.Roles"/> (OR) и требование
/// аутентифицированности. Любой principal с подписанным контекстом проходит, если нет ролей.
/// </summary>
public sealed class DefaultAuthorizer : IAuthorizer
{
    public ValueTask<bool> AuthorizeAsync(
        ClaimsPrincipal? principal,
        BusAuthorizeAttribute requirement,
        ConsumeContext context)
    {
        if (principal is null)
        {
            // Если требуются конкретные роли — анонимный не проходит, даже если RequireAuthenticated == false
            if (requirement.Roles.Length > 0) return ValueTask.FromResult(false);
            return ValueTask.FromResult(!requirement.RequireAuthenticated);
        }

        var roles = requirement.Roles;
        var inRole = roles.Length == 0 || roles.Any(principal.IsInRole);

        return ValueTask.FromResult(inRole);
    }
}

/// <summary>
/// Middleware авторизации (идея 453/454): перед вызовом хендлеров восстанавливает principal
/// из конверта и проверяет требование <see cref="BusAuthorizeAttribute"/>. Отказ бросает
/// <see cref="UnauthorizedMessageException"/> — сообщение уходит в DLQ без ретраев (авторизация
/// не станет ретрайбельной).
/// </summary>
public sealed class AuthorizationMiddleware(
    DispatcherRegistry dispatchers,
    IPrincipalExtractor principalExtractor,
    IAuthorizer authorizer) : IBusMiddleware
{
    private readonly ConcurrentDictionary<Type, BusAuthorizeAttribute?> _cache = new();

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var messageType = context.Message.GetType();

        var principal = context.Principal ??= principalExtractor.Extract(context.Envelope);

        var requirement = _cache.GetOrAdd(messageType, ResolveRequirement);
        if (requirement is null)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        if (!await authorizer.AuthorizeAsync(principal, requirement, context).ConfigureAwait(false))
            throw new UnauthorizedMessageException(context.Envelope.MessageType, requirement);

        await next(context).ConfigureAwait(false);
    }

    /// <summary>
    /// Требование берётся с любого обработчика сообщения. Разные хендлеры могут требовать
    /// разное — используем первое: если хотя бы один хендлер защищён, сообщение не обрабатывается
    /// анонимно. В будущем — OR-аккумуляция проверок.
    /// </summary>
    private BusAuthorizeAttribute? ResolveRequirement(Type messageType)
    {
        foreach (var dispatcher in dispatchers.For(messageType))
        {
            if (dispatcher is IHandlerAuthorizationProvider { Authorization: { } auth })
                return auth;
        }

        return null;
    }
}

/// <summary>Principal не прошёл авторизацию: сообщение не обрабатывается (идея 453).</summary>
public sealed class UnauthorizedMessageException(string messageType, BusAuthorizeAttribute requirement)
    : Exception(
        $"Сообщение '{messageType}' отклонено авторизацией: " +
        $"{(requirement.Policy is null ? "" : $"policy={requirement.Policy} ")}" +
        $"roles=[{string.Join(",", requirement.Roles)}]");
