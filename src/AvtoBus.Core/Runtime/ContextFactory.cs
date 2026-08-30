using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace AvtoBus.Runtime;

/// <summary>
/// Строит <see cref="ConsumeContext{T}"/> нужного типа. Конструктор internal и обобщённый,
/// поэтому вызов компилируется в делегат один раз на тип сообщения — не на каждое сообщение.
/// </summary>
internal static class ContextFactory
{
    private delegate ConsumeContext Constructor(
        Envelope envelope,
        object message,
        IServiceProvider services,
        IBus bus,
        CancellationToken ct,
        TransportDestination source);

    private static readonly ConcurrentDictionary<Type, Constructor> Constructors = new();

    public static ConsumeContext Create(
        Type messageType,
        Envelope envelope,
        object message,
        IServiceProvider services,
        IBus bus,
        CancellationToken ct,
        TransportDestination source)
        => Constructors.GetOrAdd(messageType, Build)(envelope, message, services, bus, ct, source);

    private static Constructor Build(Type messageType)
    {
        var contextType = typeof(ConsumeContext<>).MakeGenericType(messageType);

        var constructor = contextType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            [typeof(Envelope), messageType, typeof(IServiceProvider), typeof(IBus), typeof(CancellationToken)],
            modifiers: null)
            ?? throw new InvalidOperationException($"Не найден конструктор {contextType.Name}.");

        var envelope = Expression.Parameter(typeof(Envelope), "envelope");
        var message = Expression.Parameter(typeof(object), "message");
        var services = Expression.Parameter(typeof(IServiceProvider), "services");
        var bus = Expression.Parameter(typeof(IBus), "bus");
        var ct = Expression.Parameter(typeof(CancellationToken), "ct");
        var source = Expression.Parameter(typeof(TransportDestination), "source");

        // Source — init-only свойство базового класса, задаётся через инициализатор объекта.
        var sourceProperty = typeof(ConsumeContext).GetProperty(nameof(ConsumeContext.Source))!;

        Expression body = Expression.MemberInit(
            Expression.New(
                constructor,
                envelope,
                Expression.Convert(message, messageType),
                services,
                bus,
                ct),
            Expression.Bind(sourceProperty, source));

        return Expression.Lambda<Constructor>(
            Expression.Convert(body, typeof(ConsumeContext)),
            envelope, message, services, bus, ct, source).Compile();
    }
}
