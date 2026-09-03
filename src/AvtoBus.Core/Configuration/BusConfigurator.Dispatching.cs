using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization;
using AvtoBus.Handlers;
using AvtoBus.Pipeline;
using AvtoBus.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Configuration;

internal static class ServiceCollectionHandlerExtensions
{
    /// <summary>
    /// Регистрирует тип-хендлер как scoped, не дублируя уже существующую регистрацию.
    /// Статические классы не регистрируются: их методы вызываются напрямую без DI (док 16 §3),
    /// а AddScoped(static) падает при построении контейнера.
    /// </summary>
    public static void TryAddConsumerService(
        this IServiceCollection services,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type type)
    {
        if (type.IsAbstract && type.IsSealed)
            return;

        if (services.Any(descriptor => descriptor.ServiceType == type))
            return;

        services.AddScoped(type);
    }
}

/// <summary>Диспетчер для классов, реализующих <see cref="IConsumer{T}"/>.</summary>
internal sealed class InterfaceDispatcher : IMessageDispatcher, IHandlerTimeoutProvider, IHandlerAuthorizationProvider
{
    private readonly Type _handlerType;
    private readonly Func<object, ConsumeContext, Task> _invoke;
    private readonly MethodInfo _method;

    [RequiresUnreferencedCode(
        "Компиляция вызова через Expression — reflection-путь (legacy). Под AOT этот диспетчер " +
        "не создаётся: IConsumer<T> покрывается генератором.")]
    public InterfaceDispatcher(Type handlerType, Type messageType, MethodInfo method)
    {
        _handlerType = handlerType;
        MessageType = messageType;
        HandlerName = $"{handlerType.Name}.{method.Name}";
        _method = method;

        var contextType = typeof(ConsumeContext<>).MakeGenericType(messageType);
        var handler = System.Linq.Expressions.Expression.Parameter(typeof(object), "handler");
        var context = System.Linq.Expressions.Expression.Parameter(typeof(ConsumeContext), "ctx");

        var call = System.Linq.Expressions.Expression.Call(
            System.Linq.Expressions.Expression.Convert(handler, handlerType),
            method,
            System.Linq.Expressions.Expression.Convert(context, contextType));

        _invoke = System.Linq.Expressions.Expression
            .Lambda<Func<object, ConsumeContext, Task>>(call, handler, context)
            .Compile();
    }

    public Type MessageType { get; }

    public string HandlerName { get; }

    public TimeSpan? Timeout
        => _method.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout
           ?? _handlerType.GetCustomAttribute<HandlerTimeoutAttribute>()?.Timeout;

    public BusAuthorizeAttribute? Authorization
        => _method.GetCustomAttribute<BusAuthorizeAttribute>()
           ?? _handlerType.GetCustomAttribute<BusAuthorizeAttribute>();

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var handler = context.Services.GetRequiredService(_handlerType);
        await _invoke(handler, context).ConfigureAwait(false);
    }
}
