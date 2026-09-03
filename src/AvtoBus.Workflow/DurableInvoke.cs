namespace AvtoBus.Workflow;

/// <summary>
/// Durable вызов дочернего workflow с возвратом результата.
/// В отличие от `Canvas.Chain` (fire-and-forget), `Invoke` ждет завершения child и возвращает `TOut` в parent, переживая рестарт.
/// </summary>
public static class DurableInvokeExtensions
{
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Дочерний workflow резолвится по имени типа через сканирование сборок — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Дочерний workflow создаётся через Activator по имени типа — несовместимо с NativeAOT.")]
    public static Task<TOut> InvokeChildAsync<TIn, TOut>(this IWorkflowContext ctx, string workflowType, TIn input, CancellationToken ct = default)
        => ctx.ExecuteActivityAsync(async () =>
        {
            var workflow = ResolveWorkflow<TIn, TOut>(workflowType);
            return await workflow.RunAsync(input, ctx).ConfigureAwait(false);
        });

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Дочерний workflow резолвится по имени типа через сканирование сборок — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Дочерний workflow создаётся через Activator по имени типа — несовместимо с NativeAOT.")]
    public static Task<TOut> InvokeChildAsync<TOut>(this IWorkflowContext ctx, AvtoWorkflow<object, TOut> workflow, object input, CancellationToken ct = default)
        => ctx.InvokeChildAsync<object, TOut>(workflow.GetType().Name, input, ct);

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Резолв типа workflow по имени — только из InvokeChildAsync.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Создание workflow через Activator — только из InvokeChildAsync.")]
    private static AvtoWorkflow<TIn, TOut> ResolveWorkflow<TIn, TOut>(string workflowType)
    {
        var type = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a => { try { return a.GetTypes(); } catch { return []; } })
            .FirstOrDefault(t => t.FullName == workflowType || t.Name == workflowType && typeof(AvtoWorkflow<TIn, TOut>).IsAssignableFrom(t))
            ?? Type.GetType(workflowType) ?? throw new InvalidOperationException($"Child workflow type '{workflowType}' not found.");
        var instance = Activator.CreateInstance(type) as AvtoWorkflow<TIn, TOut>
            ?? throw new InvalidOperationException($"Type '{workflowType}' is not AvtoWorkflow<{typeof(TIn).Name},{typeof(TOut).Name}>.");
        return instance;
    }
}
