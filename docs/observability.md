# Наблюдаемость

## Метрики (OpenTelemetry, messaging-конвенции)

`consume.duration`, `critical.time`, `publish/consume.bytes`, `pipeline.step.duration`,
`canary.rtt`, `queue.depth`, `consumer.lag`, `outbox.pending`, `outbox.oldest_pending_age`,
`dlq.size`. Решения recoverability — события трейса `avtobus.recoverability`;
спан обработки живёт на все ретраи.

## Логи и трейсы

- Скоуп каждого лога: `MessageId/CorrelationId/MessageType/Attempt`.
- `AvtoBus-Diagnostics` EventSource — для `dotnet-trace`/`dotnet-counters`.
- Аудит «кто послал»: заголовок `avtobus-initiator`.
- `RateLimitedLogger` гасит лог-штормы; канарейка `UseCanary` — живой e2e-healthcheck.

## Health и алерты

- `AddAvtoBusHealthCheck()` — стандартный `IHealthCheck` (`ready`/`live` теги
  настраиваются на стороне приложения).
- Готовые алерты Prometheus: `build/deploy/prometheus/alerts.yaml`
  (включая `AvtoBusOutboxOldestStuck`), Grafana-панели рядом, разбор инцидентов —
  `build/deploy/RUNBOOK.md`.

## Дашборд

Встраиваемый `AvtoBus.Dashboard` (`/bus`): обзор, DLQ (за auth-политикой).
Опасные действия в Production требуют явного `AllowDangerousOperationsInProduction`.
