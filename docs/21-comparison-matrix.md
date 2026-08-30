# 📋 Матрица сравнения с конкурентами

> **Target-state matrix.** Колонка AvtoBus показывает запланированный объём v1, а не текущую реализацию. Данные о конкурентах требуют проверки по официальной документации перед публикацией.

Легенда: ✅ заявлено в целевой спецификации · 🟡 частично / плагином · ❌ не заявлено · 💰 коммерческая функция

## 1. Основа фреймворка

| Возможность | **AvtoBus target v1** | MassTransit | NServiceBus | Wolverine | Rebus | CAP | Brighter |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| .NET 10/11 target | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Native AOT | ✅ | ❌ | ❌ | 🟡 | ❌ | ❌ | 🟡 |
| Zero-reflection (Source Gen) | ✅ | ❌ | ❌ | ✅ codegen | ❌ | ❌ | 🟡 |
| Method-handlers без интерфейсов | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| Интерфейсные хендлеры | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Каскадные сообщения (return = publish) | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| Compile-time диагностики | ✅ | ❌ | 🟡 | 🟡 | ❌ | ❌ | ❌ |
| Middleware-пайплайн | ✅ | ✅ фильтры | ✅ behaviors | ✅ | 🟡 | 🟡 | ✅ Russian-doll |
| Open source | ✅ MIT | ✅ Apache | 💰 | ✅ MIT | ✅ MIT | ✅ MIT | ✅ BSD |

## 2. Транспорты

| Транспорт | **AvtoBus** | MassTransit | NServiceBus | Wolverine | Rebus | CAP |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| RabbitMQ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Kafka | ✅ EOS | ✅ Rider | 💰 | ✅ | 🟡 | ✅ |
| Azure Service Bus | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NATS/JetStream | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ |
| Redis Streams | ✅ | ❌ | ❌ | ❌ | 🟡 | ✅ |
| SQL-транспорт (PG/MSSQL) | ✅ | 🟡 | ✅ | ✅ Postgres | ✅ | 🟡 |
| SQS/SNS | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Google Pub/Sub | ✅ | ✅ | ❌ | 🟡 | 🟡 | 🟡 |
| In-Memory (полная семантика) | ✅ | ✅ | 💰 | ✅ | ✅ | ❌ |
| File / Append-log | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Multi-transport один процесс | ✅ | ✅ | 🟡 | ✅ | 🟡 | 🟡 |
| Conformance-kit транспортов | ✅ | ❌ | ❌ | 🟡 | ❌ | ❌ |

## 3. Надёжность

| Возможность | **AvtoBus** | MassTransit | NServiceBus | Wolverine | CAP |
|---|:-:|:-:|:-:|:-:|:-:|
| Transactional Outbox (EF Core) | ✅ | ✅ v9 | ✅ | ✅ | ✅ |
| Inbox-дедупликация | ✅ | 🟡 | ✅ | ✅ | 🟡 |
| Immediate + delayed retries | ✅ | ✅ | ✅ | ✅ | ✅ |
| Per-exception retry policy | ✅ | ✅ | ✅ | ✅ | 🟡 |
| Circuit breaker per-consumer | ✅ | 🟡 | ✅ | ✅ | ❌ |
| Retry budgets (thundering herd) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Poison vs error queue разделение | ✅ | 🟡 | ✅ | 🟡 | ❌ |
| DLQ реплей с фильтром/rate-limit | ✅ | 🟡 | ✅ ServicePulse | 🟡 | ✅ dashboard |
| Second-level retry (IFailed<T>) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Fencing tokens | ✅ | ❌ | ❌ | ❌ | ❌ |
| Effectively-once на handler | ✅ | 🟡 | ✅ | ✅ | 🟡 |
| Exactly-once Kafka транзакции | ✅ | 🟡 | ❌ | 🟡 | ❌ |

## 4. Саги / Workflow

| Возможность | **AvtoBus** | MassTransit | NServiceBus | Wolverine | Temporal |
|---|:-:|:-:|:-:|:-:|:-:|
| Class-based saga с корреляцией | ✅ | ✅ | ✅ | ✅ | ❌ |
| State-machine DSL | ✅ | ✅ Automatonymous | ❌ | 🟡 | ❌ |
| Durable execution (code as workflow) | ✅ | ❌ | ❌ | ❌ | ✅ |
| Компенсации first-class | ✅ | ✅ Courier | 🟡 | 🟡 | ✅ |
| Routing slip | ✅ | ✅ | ❌ | ❌ | ❌ |
| Timeouts как first-class | ✅ | ✅ | ✅ | ✅ | ✅ |
| Business calendar для таймаутов | ✅ | ❌ | ❌ | ❌ | 🟡 |
| SLA-мониторы процессов | ✅ | ❌ | 💰 ServicePulse | ❌ | 🟡 |
| Экспорт диаграммы (Mermaid/BPMN) | ✅ | ❌ | ❌ | ❌ | 🟡 |
| Human-in-the-loop | ✅ | ❌ | ❌ | ❌ | ✅ |
| Sub-sagas / child workflows | ✅ | 🟡 | ✅ | ❌ | ✅ |
| Cron + distributed leader | ✅ | 🟡 | 🟡 | ✅ | ✅ |

## 5. Event Sourcing

| Возможность | **AvtoBus** | MassTransit | Marten | Axon (Java) | EventStoreDB |
|---|:-:|:-:|:-:|:-:|:-:|
| Event store PostgreSQL | ✅ | ❌ | ✅ | 🟡 | ❌ |
| Snapshots с политиками | ✅ | ❌ | ✅ | ✅ | ❌ |
| Upcasters | ✅ | ❌ | 🟡 | ✅ | ❌ |
| Inline / async / live projections | ✅ | ❌ | ✅ | ✅ | ✅ |
| Реплей проекций онлайн | ✅ blue/green | ❌ | ✅ | ✅ | ✅ |
| Crypto-shredding (GDPR) | ✅ | ❌ | 🟡 | ✅ | ❌ |
| Time-travel запросы | ✅ | ❌ | ✅ | ✅ | ✅ |
| Множ. read-моделей (ES/Redis/CH) | ✅ | ❌ | 🟡 | ✅ | 🟡 |
| Hash-chain immutability | ✅ | ❌ | ❌ | 🟡 | ❌ |
| Интеграция ES ↔ шина | ✅ через outbox | — | 🟡 | ✅ | 🟡 |

## 6. Observability & DevEx

| Возможность | **AvtoBus** | MassTransit | NServiceBus | Wolverine | CAP |
|---|:-:|:-:|:-:|:-:|:-:|
| OpenTelemetry OOTB | ✅ | ✅ | ✅ | ✅ | ✅ |
| Стандартные messaging semconv | ✅ | ✅ | 🟡 | 🟡 | 🟡 |
| Дашборд (web UI) | ✅ Blazor | ❌ | 💰 | ❌ | ✅ |
| Live граф топологии | ✅ | ❌ | 💰 | ❌ | ❌ |
| DLQ браузер с реплеем | ✅ | ❌ | 💰 | 🟡 | ✅ |
| Live-tail сообщений | ✅ | ❌ | ❌ | ❌ | ❌ |
| Тест-харнесс first-class | ✅ | ✅ | ✅ | ✅ | 🟡 |
| Виртуальное время в тестах | ✅ | 🟡 | ❌ | 🟡 | ❌ |
| Chaos-middleware для тестов | ✅ | ❌ | ❌ | ❌ | ❌ |
| CLI (dotnet tool) | ✅ | ❌ | 💰 | 🟡 | ❌ |
| Roslyn analyzers + code-fixes | ✅ | ❌ | ❌ | 🟡 | ❌ |
| .NET Aspire интеграция | ✅ | 🟡 | 🟡 | 🟡 | 🟡 |
| Templates (dotnet new) | ✅ | 🟡 | ✅ | ✅ | 🟡 |
| AsyncAPI автогенерация | ✅ | ❌ | ❌ | ❌ | ❌ |
| Doc-tests (код в доках компилится) | ✅ | ❌ | ❌ | ❌ | ❌ |

## 7. Security & Enterprise

| Возможность | **AvtoBus** | MassTransit | NServiceBus | Wolverine |
|---|:-:|:-:|:-:|:-:|
| Подпись сообщений (HMAC/Ed25519) | ✅ | ❌ | ❌ | ❌ |
| Envelope encryption + KMS | ✅ | ❌ | ❌ | ❌ |
| Мультитенантность (3 уровня) | ✅ | 🟡 | 🟡 | 🟡 |
| Fair scheduling между тенантами | ✅ | ❌ | ❌ | ❌ |
| Data residency роутинг | ✅ | ❌ | ❌ | ❌ |
| PII-теги + автомаскирование | ✅ | ❌ | ❌ | ❌ |
| Rate limit per-principal | ✅ | ❌ | ❌ | ❌ |
| Break-glass + аудит | ✅ | ❌ | ❌ | ❌ |
| Legal hold ретеншн | ✅ | ❌ | ❌ | ❌ |
| SOC2/ISO комплаенс-отчёты | ✅ | ❌ | 💰 | ❌ |

## 8. Кросс-язык (что мы берём и у кого)

| Идея источника | Язык | В **AvtoBus** |
|---|---|:-:|
| Method-handlers + codegen (Wolverine) | C# | ✅ ядро |
| Аннотированные агрегаты (Axon) | Java | ✅ ES |
| Kafka Streams DSL | Java | ✅ мини-DSL стримов |
| Watermill Router+Middleware | Go | ✅ пайплайн |
| Temporal durable execution | Go/Java | ✅ саги |
| NATS wildcard subjects | Go | ✅ |
| Broadway back-pressure/batching | Elixir | ✅ |
| Oban unique jobs / cron | Elixir | ✅ |
| BullMQ flows | JS | ✅ canvas |
| Celery chain/group/chord | Python | ✅ |
| FastStream AsyncAPI автодок | Python | ✅ |
| Tower Service/Layer композиция | Rust | ✅ middleware |
| NestJS декораторы каналов | JS | ✅ атрибуты |
| Spring Modulith externalized events | Java | ✅ модульный монолит |
| Redpanda WASM трансформации | Go/Rust | ✅ плагины |

## 9. Итоговое позиционирование

**AvtoBus = Wolverine-DX + NServiceBus-надёжность + Axon-ES + Temporal-саги + FastStream-документация**, всё open source, всё AOT, всё модульно, всё с батарейками.

Клюшка ценностного предложения:
> «Если начинаешь новый .NET-микросервис сегодня — начни на AvtoBus.
> Простое остаётся простым, сложное становится возможным без смены фреймворка.»
