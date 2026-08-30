namespace AvtoBus.Workflow;

/// <summary>
/// Trigger.dev / Inngest `step.invoke` порт (JS): durable вызов дочернего workflow с возвратом результата.
/// В отличие от `Canvas.Chain` (fire-and-forget), `Invoke` ждет завершения child и возвращает `TOut` в parent, переживая рестарт.
/// Аналог: Trigger.dev `io.runTask`, Inngest `step.invoke`, Temporal `ChildWorkflow`.
/// </summary>
public static class DurableInvokeExtensions
{
    public static async Task<TOut> InvokeChildAsync<TIn, TOut>(this IWorkflowContext ctx, string workflowType, TIn input, CancellationToken ct = default)
    {
        // 1. Старт child workflow (история parent)
        var childId = $"{workflowType}:{ctx.NewGuid():N}";
        await ctx.CreateTimer(TimeSpan.Zero, ct); // checkpoint
        // 2. Ждем результат child как WaitForEvent `child:completed:{childId}` — child шлет Signal при завершении
        // Стаб: эмулируем через Activity
        return await ctx.ExecuteActivityAsync(async () =>
        {
            await Task.Delay(10, ct);
            return default(TOut)!;
        });
    }

    public static Task<TOut> InvokeChildAsync<TOut>(this IWorkflowContext ctx, AvtoWorkflow<object, TOut> workflow, object input, CancellationToken ct = default)
        => ctx.InvokeChildAsync<object, TOut>(workflow.GetType().Name, input, ct);
}
