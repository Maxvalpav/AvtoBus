# Verification Matrix AvtoBus MVP

> **Статус: Planning / quality gate.** Ни одно свойство не считается реализованным, пока соответствующая строка не имеет реальный test id и зелёный CI artifact.

## Уровни проверки

| Уровень | Что доказывает |
|---|---|
| U | Unit: локальная логика без I/O |
| C | Compile/generator: diagnostics и generated source |
| T | Transport conformance: общий контракт provider-а |
| I | Integration: реальный broker/DB через Testcontainers |
| F | Failure injection: crash, timeout, ambiguous result |
| A | AOT/trim: publish и runtime smoke test |
| P | Performance: воспроизводимый benchmark artifact |
| S | Security: threat-model test/review |

## Core и dispatch

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Method handler вызывается без reflection | C, A | Generated dispatcher содержит direct call; AOT без reflection warning | Not implemented |
| Command имеет ровно один handler | C | AVB001/AVB002 compile tests | Not implemented |
| Event может иметь N handlers | C, U | Все generated subscribers вызваны | Not implemented |
| Scope создаётся один раз на attempt | U, I | Scoped probe identity одинакова во всём handler pipeline | Not implemented |
| Cancellation доходит до handler | U, I | Host stop/timeout отменяет token | Not implemented |
| Cascade использует текущий session | I | Outgoing row в той же transaction | Not implemented |

## Delivery и transport

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| At-least-once delivery | T, F | Crash до ack приводит к redelivery | Not implemented |
| Ack после commit | I, F | Crash после commit до ack создаёт дубль, не потерю | Not implemented |
| Nack requeue работает | T | Повторная delivery с attempt +1 | Not implemented |
| Nack terminal идёт в DLQ | T, I | Rich error metadata сохранена | Not implemented |
| Back-pressure bounded | T, P | Flood не увеличивает память без границ | Not implemented |
| Partition order | T, F | Sequence per key монотонна при concurrency | Not implemented |
| Graceful drain | I, F | In-flight завершены или requeue до shutdown deadline | Not implemented |

## Outbox и Inbox

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Business data + outbox атомарны | I, F | Rollback обоих при exception | Not implemented |
| Relay переживает crash | I, F | Committed pending row отправляется после restart | Not implemented |
| Ambiguous publish не повторяет effect | I, F | Duplicate delivery подавлена Inbox | Not implemented |
| Несколько relay replicas безопасны | I, P | `SKIP LOCKED`, нет одновременного claim одной row | Not implemented |
| Consumer identity стабильна | I | Две replicas используют один inbox key namespace | Not implemented |
| Inbox retention контролируема | I | Cleanup не удаляет entries моложе window | Not implemented |

## Retry и recoverability

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Immediate retry ограничен | U | Ровно N попыток | Not implemented |
| Delayed retry durable | I, F | Restart во время delay не теряет retry | Not implemented |
| Permanent exception не retry | U, I | Validation error сразу terminal outcome | Not implemented |
| Retry jitter ограничен | U | Delay внутри documented range | Not implemented |
| Retry budget защищает downstream | U, P | Retry traffic не превышает budget | Post-MVP |

## Request/Reply

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Быстрый reply не теряется | I | Waiter зарегистрирован до publish | Not implemented |
| Timeout очищает waiter | U, I | Registry count возвращается к 0 | Not implemented |
| Cancellation очищает waiter | U, I | Registry count возвращается к 0 | Not implemented |
| Late reply не requeue | I | Metric increment + ack | Not implemented |
| Replicas изолированы | I | Instance reply endpoint routing | Not implemented |

## Observability

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Trace propagation | I | publish span связан с consume span | Not implemented |
| Correlation propagation | U, I | Формула из wire protocol section 6 | Not implemented |
| Metrics bounded cardinality | U, P | MessageId/TenantId не используются как metric tags | Not implemented |
| Payload redaction | S, U | `[PersonalData]` не попадает в logs/dashboard | Not implemented |

## AOT и производительность

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Core + InMemory AOT compatible | A | Native publish + smoke run, zero warnings | Not implemented |
| RabbitMQ AOT compatible | A, I | Native smoke test с broker | Not implemented |
| Allocation SLO | P | Raw BenchmarkDotNet artifact | TBD |
| Latency SLO | P | Environment/topology/payload зафиксированы | TBD |

## Security

| Claim | Test level | Обязательная проверка | Статус |
|---|---|---|---|
| Allowlist deserialization | S, U | Unknown type не загружает CLR type | Not implemented |
| Signature verification | S, U, I | Tampered body/header отклоняются | Not implemented |
| Replay protection | S, I | Duplicate signed message подавляется Inbox | Not implemented |
| Tenant isolation | S, I | Cross-tenant storage read/write запрещены | Not implemented |
| Dashboard dangerous actions protected | S, I | Auth policy + audit row required | Not implemented |

## Release gate

MVP release разрешён, когда:

1. Все строки Core, Delivery, Outbox/Inbox и Observability имеют статус `Pass`.
2. RabbitMQ проходит transport conformance suite.
3. AOT gate Core + InMemory зелёный.
4. Нет unresolved Critical/High findings threat model.
5. Benchmark SLO либо подтверждены, либо скорректированы ADR-ом без скрытого изменения результатов.
6. Raw CI artifacts доступны по release commit SHA.

## Закрыто в текущей итерации (B3, B5, B6, B10, B11, B12, B13)

| Claim | Test | Status |
|---|---|---|
| Local queue доставляет по имени/маршруту | `AvtoBus.Tests.Local.LocalQueueTests.EnqueueLocal_delivers_to_explicit_queue`, `..._resolves_destination_via_routing` | Pass |
| Локальная очередь без транспорта падает понятно | `...EnqueueLocal_without_local_transport_fails_loudly` | Pass |
| Ретраи локального консьюмера → error-очередь | `...Failed_local_consumer_retries_then_deadletters_to_error_queue` | Pass |
| Back-pressure локальной очереди bounded | `...Backpressure_blocks_writer_when_local_queue_is_full` | Pass |
| Routing `ToQueue(...).Via(...)` учитывает транспорт | `LocalQueueTests` (регрессия `RoutingTable.ResolveCore`) | Pass |
| Секция "AvtoBus" биндится в IOptions | `AvtoBus.Tests.ConfigurationBinding.ConfigurationTests.AddAvtoBus_binds_avtobus_section_to_options` | Pass |
| Конфиг применяется к конфигуратору | `...AddAvtoBus_applies_configuration_to_configurator` | Pass |
| Невалидная конфигурация — fail-fast | `...Invalid_configuration_fails_startup` | Pass |
| OTel подписка на ActivitySource шины | `AvtoBus.Tests.Observability.OpenTelemetryExtensionTests.AddAvtoBusInstrumentation_subscribes_tracer_to_bus_source` | Pass |
| OTel подписка на Meter шины | `...AddAvtoBusInstrumentation_subscribes_meter_to_bus_instruments` | Pass |
| MessagePack round-trip контракта | `AvtoBus.Tests.Serialization.BinarySerializerTests.MessagePack_round_trips_contract` | Pass |
| MessagePack как дефолт шины | `...MessagePack_as_default_delivers_over_the_bus` | Pass |
| Content-Type диспетчеризация бинарного формата | `...MessagePack_content_type_is_recognized_by_registry` | Pass |
| Protobuf round-trip сгенерированного контракта | `...Protobuf_round_trips_generated_message` | Pass |
| Protobuf отклоняет не-protobuf контракт | `...Protobuf_rejects_non_protobuf_contract` | Pass |
| Protobuf как дефолт шины | `...Protobuf_as_default_delivers_over_the_bus` | Pass |
| CloudEvents проставляет ce-* атрибуты | `AvtoBus.Tests.Observability.CloudEventsAndClaimCheckTests.CloudEvents_adds_ce_attributes_to_outbound_envelope` | Pass |
| CloudEvents использует явный source | `...CloudEvents_uses_explicit_source` | Pass |
| CloudEvents выключен по умолчанию | `...CloudEvents_disabled_by_default` | Pass |
| Claim Check: большой payload в blob и обратно | `...Large_message_goes_to_blob_store_and_back` | Pass |
| Claim Check: заголовок ссылки виден хендлеру | `...Large_message_leaves_claim_check_header_visible_to_handler` | Pass |
| Claim Check: маленькое сообщение остаётся inline | `...Small_message_stays_inline` | Pass |
| CLI dlq: дерево команд собирается и диспетчится | `AvtoBus.Tests.Cli.CliDlqCommandTests.Status_...`, `...List_...`, `...Replay_without_id_...`, `...Replay_of_missing_message_...` | Pass |
| OrderedBy: порядок по ключу через receive-side селектор | `AvtoBus.Tests.Reliability.PartitionOrderingTests.OrderedBy_keeps_each_key_in_order_via_selector` | Pass |
| OrderedBy: порядок по `[PartitionKey]` конверта | `...OrderedBy_uses_envelope_partition_key_when_present` | Pass |
| SchemaMigrator применяет в порядке (module, version) | `AvtoBus.Tests.Migrations.SchemaMigratorTests.Applies_pending_migrations_in_module_then_version_order` | Pass |
| SchemaMigrator пропускает применённое | `...Skips_already_applied_migrations` | Pass |
| SchemaMigrator идемпотентен между рестартами | `...Idempotent_across_restarts` | Pass |
| SchemaMigrator докатывает только новые версии | `...Applies_only_newer_versions_when_partially_migrated` | Pass |
| UseOutbox поднимает схему модуля при старте хоста | `AvtoBus.Tests.OutboxPostgresTests.UseOutbox_ensures_module_schema_on_host_start` | Pass (PG) |

## Формат ссылки на доказательство

После появления tests каждая строка получает:

```text
Status: Pass
Test: AvtoBus.Outbox.PostgresTests.CrashAfterCommitBeforeAck
CI: https://.../actions/runs/<id>
Artifact: outbox-failure-matrix.trx
Last verified: <commit SHA>
```