using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Security;

/// <summary>
/// Служба фоновой ротации ключей (идея 452): периодически вызывает <c>RotateKeysIfDue</c>,
/// чтобы все инстансы кластера перешли на очередную эпоху ключей без рестарта.
/// Между ротациями — спокойный сон, без таймеров.
/// </summary>
public sealed class SecurityKeyRotationService(
    TimeProvider time,
    EnvelopeSecurity security,
    TimeSpan rotationInterval,
    ILogger<SecurityKeyRotationService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(rotationInterval, time, stoppingToken).ContinueWith(_ => { }, stoppingToken);
            if (stoppingToken.IsCancellationRequested)
                break;

            security.RotateKeysIfDue(time.GetUtcNow());
            logger.LogDebug("Ключи безопасности ротированы (новая эпоха)");
        }
    }
}
