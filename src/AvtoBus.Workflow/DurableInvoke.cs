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
        // Child invocation not yet fully implemented — throw to avoid silent incorrect behavior.
        // Parent should use ctx.ExecuteActivityAsync with explicit compensation instead.
        throw new NotImplementedException($"DurableInvoke.InvokeChildAsync<{typeof(TIn).Name},{typeof(TOut).Name}> not implemented: workflow '{workflowType}' child invocation requires workflow runner integration. Use ExecuteActivityAsync as workaround.");
    }

    public static Task<TOut> InvokeChildAsync<TOut>(this IWorkflowContext ctx, AvtoWorkflow<object, TOut> workflow, object input, CancellationToken ct = default)
        => ctx.InvokeChildAsync<object, TOut>(workflow.GetType().Name, input, ct);
}
