namespace AvtoBus.Pipeline;

public delegate ValueTask BusDelegate(ConsumeContext context);

/// <summary>
/// Шаг пайплайна обработки. «Русская матрёшка», как в ASP.NET Core: вызвал <paramref name="next"/> —
/// пустил дальше, не вызвал — оборвал цепочку.
/// </summary>
public interface IBusMiddleware
{
    ValueTask InvokeAsync(ConsumeContext context, BusDelegate next);
}

/// <summary>Middleware из лямбды — для мелочей, ради которых не хочется заводить класс.</summary>
public sealed class DelegateMiddleware(Func<ConsumeContext, BusDelegate, ValueTask> handler) : IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next) => handler(context, next);
}
