# AvtoBus Runbook (prod)

## Аварийный readonly
```bash
avtobus readonly on   # создаёт ~/.config/avtobus/readonly → BusOptions.IsReadOnly = true
avtobus readonly status
avtobus readonly off
# или env
AVTOBUS_READONLY=1 dotnet run
```
Исходящие каскады подавляются, обработка входящих остаётся, метрика `readonly-suppressed`.

## DLQ / poison
```bash
avtobus dlq list --transport rabbitmq
# реплей — через шину: bus.RepublishAsync(envelope)
```

## Канарейка / пробы
- `/healthz` liveness, `/readyz` readiness, `/startupz` startup (8080).
- OTel: `avtobus.queue.depth`, `consumer.lag`, `outbox.pending`, `dlq.size`, `consume.duration`.

## Масштабирование
- KEDA `build/k8s/keda-scaledobject.yaml` и `helm/values.yaml:keda` — по `queueName`.
- Helm: `helm upgrade --install avtobus ./build/deploy/helm`

## SBOM
`build/deploy/generate-sbom.ps1 -ArtifactPath ./artifacts -OutputPath ./sbom && syft ./artifacts -o spdx-json=sbom.spdx.json`
