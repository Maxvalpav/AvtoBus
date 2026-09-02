using AvtoBus.Configuration;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Runtime;

/// <summary>
/// Крон-«канарейка» (идея 337): системное сообщение каждые N секунд проходит
/// полный путь publish → транспорт → consume. Время полного цикла — живой
/// end-to-end healthcheck: если транспорт/сериализация сильно деградировали,
/// канарейка первая покажет рост (или пропадёт вовсе).
///
/// Идемпотентность: у каждого цикла свой MessageId, совпадение проверяется
/// по заголовку — чужие сообщения из очереди не принимаются за свою канарейку.
/// </summary>
public sealed class CanaryProbe(
    BusOptions options,
    TransportRegistry transports,
    EnvelopeFactory envelopes,
    ILogger<CanaryProbe> logger) : BackgroundService
{
    private static string DestinationName(string serviceName) => $"avtobus.canary.{serviceName}";

    private static string ConsumerGroup => $"avtobus-canary/{Environment.MachineName}/{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Canary-канарейка запущена: каждые {Interval}", options.CanaryInterval);

        // Сразу после старта — базовый замер, дальше по расписанию.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await FlyAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                BusTelemetry.CanaryFailed(options.ServiceName);
                logger.LogError(exception, "Канарейка не долетела: {Message}", exception.Message);
            }

            try
            {
                await Task.Delay(options.CanaryInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>Один цикл: отправить канарейку, принять её обратно, замерить полное время.</summary>
    private async Task FlyAsync(CancellationToken ct)
    {
        var destination = TransportDestination.Topic(DestinationName(options.ServiceName));

        await transports.Default.ProvisionAsync([destination], ct).ConfigureAwait(false);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var id = Guid.NewGuid();
        var envelope = envelopes.CreateForCanary(id);

        await transports.Default.SendAsync(envelope, destination, ct).ConfigureAwait(false);

        var ok = await TryReceiveOwnAsync(destination, id, ct).ConfigureAwait(false);
        var elapsed = sw.Elapsed;

        if (ok)
        {
            BusTelemetry.CanaryCompleted(elapsed.TotalMilliseconds);
            logger.LogDebug("Канарейка долетела за {Ms} ms", elapsed.TotalMilliseconds);
        }
        else
        {
            BusTelemetry.CanaryTimeout(options.ServiceName, elapsed);
        }
    }

    /// <summary>Принимает сообщения канарейки, пока не встретит своё по MessageId (или не истечёт таймаут).</summary>
    private async Task<bool> TryReceiveOwnAsync(TransportDestination destination, Guid id, CancellationToken ct)
    {
        var subscription = new TransportSubscription(destination, ConsumerGroup, PrefetchCount: 1);

        using var lease = CancellationTokenSource.CreateLinkedTokenSource(ct);
        lease.CancelAfter(options.CanaryTimeout);

        try
        {
            await foreach (var message in transports.Default.ReceiveAsync(subscription, lease.Token).ConfigureAwait(false))
            {
                try
                {
                    if (message.Envelope.MessageId == id)
                    {
                        await message.AcknowledgeAsync(ct).ConfigureAwait(false);
                        return true;
                    }

                    // Not ours: просто ack без ре-публикации — чужую канарейку (другая реплика/сервис) не гоняем по кругу.
                    await message.AcknowledgeAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }

            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
