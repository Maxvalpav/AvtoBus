using System.Runtime.CompilerServices;

namespace AvtoBus.Handlers;

/// <summary>
/// Разбирает то, что вернул хендлер, в набор каскадных сообщений (идея 2).
/// Поддерживается: <c>null</c>, одиночное сообщение, кортеж, коллекция,
/// <see cref="OutgoingMessages"/> и <see cref="Result{T}"/>.
/// </summary>
public static class HandlerResult
{
    /// <summary>
    /// Раскладывает возвращённое значение по <paramref name="context"/>.
    /// Возвращает <c>false</c>, если хендлер вернул бизнес-отказ — тогда каскадов нет.
    /// </summary>
    public static bool Apply(ConsumeContext context, object? returned)
    {
        if (returned is null)
            return true;

        switch (returned)
        {
            case Result result:
                return ApplyResult(context, result, null);

            case OutgoingMessages outgoing:
                foreach (var message in outgoing)
                    context.Enqueue(message);
                return true;

            case ITuple tuple:
                for (var i = 0; i < tuple.Length; i++)
                    Apply(context, tuple[i]);
                return true;

            case string:
                // Строка — это IEnumerable, но точно не набор сообщений. Ловим до общей ветки.
                throw new InvalidOperationException(
                    "Хендлер вернул string. Каскадное сообщение должно быть контрактом-типом, а не строкой.");

            case System.Collections.IEnumerable enumerable:
                foreach (var item in enumerable)
                    Apply(context, item);
                return true;
        }

        // Result<T> — обобщённый value-type, паттерн-матчингом по интерфейсу IResultValue
        // ловится без рефлексии (AOT-safe).
        if (returned is IResultValue resultValue)
            return ApplyResult(context, resultValue.Kind switch
            {
                ResultKind.Ok => Result.Ok(),
                ResultKind.Rejected => Result.Reject(resultValue.Reason ?? "rejected"),
                _ => Result.Transient(resultValue.Reason ?? "transient"),
            }, resultValue.Value);

        context.Enqueue(new OutgoingMessage(returned, KindOf(returned, context), null));
        return true;
    }

    private static bool ApplyResult(ConsumeContext context, Result result, object? value)
    {
        switch (result.Kind)
        {
            case ResultKind.Ok:
                if (value is not null)
                    Apply(context, value);
                return true;

            case ResultKind.Rejected:
                // Бизнес-отказ: ретраить нечего, решение окончательное.
                context.DeadLetter(result.Reason ?? "rejected");
                return false;

            default:
                // Транзиентная ошибка: пусть recoverability отработает свою политику.
                throw new TransientFailureException(result.Reason ?? "transient failure");
        }
    }

    /// <summary>
    /// Команда уходит через Send, событие — через Publish. Тип, не помеченный ни тем ни другим,
    /// считается событием: каскад из хендлера почти всегда «вот что произошло».
    ///
    /// Исключение — обработка запроса: если сообщение пришло с ReplyTo и возвращённый тип
    /// не является контрактом шины, это ответ на request/response, а не новое событие.
    /// </summary>
    private static OutgoingKind KindOf(object message, ConsumeContext context)
    {
        if (message is ICommand)
            return OutgoingKind.Send;

        if (message is IEvent)
            return OutgoingKind.Publish;

        return context.Envelope.ReplyTo is not null
            ? OutgoingKind.Respond
            : OutgoingKind.Publish;
    }
}

/// <summary>Временная проблема, о которой хендлер сообщил явно; ретраится по политике.</summary>
public sealed class TransientFailureException(string reason)
    : Exception($"Транзиентная ошибка: {reason}")
{
    public string Reason { get; } = reason;
}
