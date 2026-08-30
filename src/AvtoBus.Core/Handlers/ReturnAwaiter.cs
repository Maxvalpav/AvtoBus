using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace AvtoBus.Handlers;

/// <summary>
/// Ожидание результата хендлера по статическому типу возврата (идея 2). Преобразования
/// Task/ValueTask компилируются один раз при старте — на горячем пути рефлексии нет.
/// </summary>
internal sealed class ReturnAwaiter
{
    private readonly bool _isTaskLike;
    private readonly Func<object, Task>? _asTask;
    private readonly Func<object, object?>? _resultGetter;

    private ReturnAwaiter(bool isTaskLike, Func<object, Task>? asTask, Func<object, object?>? resultGetter)
    {
        _isTaskLike = isTaskLike;
        _asTask = asTask;
        _resultGetter = resultGetter;
    }

    /// <summary>Собирает ожидание по типу возврата. Рефлексия — только при старте.</summary>
    [RequiresUnreferencedCode(
        "Разбор типа возврата через рефлексию при старте — reflection-путь диспетчеров (legacy).")]
    public static ReturnAwaiter For(Type returnType)
    {
        if (returnType.IsGenericType
            && returnType.GetGenericTypeDefinition() is { } definition
            && (definition == typeof(Task<>) || definition == typeof(ValueTask<>)))
        {
            var innerType = returnType.GetGenericArguments()[0];

            return new ReturnAwaiter(
                isTaskLike: true,
                asTask: definition == typeof(ValueTask<>) ? BuildAsTask(innerType) : null,
                resultGetter: BuildResultGetter(innerType));
        }

        if (returnType == typeof(Task) || returnType == typeof(ValueTask))
            return new ReturnAwaiter(isTaskLike: true, asTask: null, resultGetter: null);

        // Синхронный метод или обычный контракт — результат сразу каскад.
        return new ReturnAwaiter(isTaskLike: false, asTask: null, resultGetter: null);
    }

    /// <summary>Ждёт результат хендлера и возвращает его полезную нагрузку. Рефлексии на этом пути нет.</summary>
    public async ValueTask<object?> AwaitAsync(object? returned)
    {
        if (!_isTaskLike)
            return returned;

        if (returned is Task task)
        {
            await task.ConfigureAwait(false);
            return _resultGetter?.Invoke(task);
        }

        if (returned is ValueTask plain)
        {
            await plain.ConfigureAwait(false);
            return null;
        }

        // ValueTask<T> (упакованный): предкомпилированный AsTask даёт Task<T> без рефлексии.
        var asTask = _asTask!(returned!);
        await asTask.ConfigureAwait(false);
        return _resultGetter?.Invoke(asTask);
    }

    [RequiresUnreferencedCode("Компиляция доступа к Task<T>.Result — reflection-путь при старте.")]
    private static Func<object, object?> BuildResultGetter(Type innerType)
    {
        var taskType = typeof(Task<>).MakeGenericType(innerType);
        var instance = Expression.Parameter(typeof(object), "task");
        var body = Expression.Convert(
            Expression.Property(
                Expression.Convert(instance, taskType),
                nameof(Task<object>.Result)),
            typeof(object));

        return Expression.Lambda<Func<object, object?>>(body, instance).Compile();
    }

    [RequiresUnreferencedCode("Компиляция ValueTask<T>.AsTask — reflection-путь при старте.")]
    private static Func<object, Task> BuildAsTask(Type innerType)
    {
        var valueTaskType = typeof(ValueTask<>).MakeGenericType(innerType);
        var instance = Expression.Parameter(typeof(object), "vt");
        var asTask = valueTaskType.GetMethod(nameof(ValueTask<object>.AsTask))!;
        var call = Expression.Call(Expression.Convert(instance, valueTaskType), asTask);

        return Expression.Lambda<Func<object, Task>>(call, instance).Compile();
    }
}
