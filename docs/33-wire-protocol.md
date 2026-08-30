# AvtoBus Wire Protocol v1

> **Статус: Specification draft.** Этот документ определяет transport-neutral wire envelope. Изменение обязательного поля до v1 требует обновления ADR и compatibility tests.

## 1. Цели

- Стабильная идентичность сообщения независимо от CLR namespace и assembly.
- Одинаковая причинность, tracing и retry metadata на всех transports.
- Forward-compatible headers.
- Безопасная allowlist-десериализация.
- Возможность canonical signing без неоднозначной сериализации.

## 2. Envelope model

```text
AvtoBusEnvelopeV1
  message_id       required UUID
  message_type     required ASCII identifier
  sent_at_utc      required RFC3339 timestamp
  content_type     required media type
  body             required bytes, may be empty only for claim-check
  correlation_id   optional UUID
  causation_id     optional UUID
  partition_key    optional UTF-8 string
  tenant_id        optional UTF-8 string
  reply_to         optional transport address
  request_id       optional UUID
  deadline_utc     optional RFC3339 timestamp
  traceparent      optional W3C Trace Context
  tracestate       optional W3C Trace Context
  baggage          optional W3C Baggage
  headers          optional string map
```

Retry attempt не является частью business identity. Transport adapter получает его из broker metadata и передаёт в runtime consume context. При republish в retry topology добавляется system header `avb-delivery-attempt`.

## 3. Message type naming

Формат:

```text
<bounded-context>.<message-name>.v<major>
```

Примеры:

```text
orders.place-order.v1
orders.order-placed.v1
billing.payment-failed.v2
```

Ограничения:

- ASCII lowercase;
- сегменты разделены точкой;
- внутри сегмента допустим kebab-case;
- CLR full name и assembly name запрещены;
- major version является частью имени;
- minor backward-compatible изменения не меняют имя.

Regex:

```regex
^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+\.v[1-9][0-9]*$
```

## 4. System headers

Все системные headers используют prefix `avb-`. Пользовательские headers с этим prefix запрещены.

| Header | Required | Значение |
|---|---:|---|
| `avb-envelope-version` | yes | `1` |
| `avb-message-id` | yes | UUID |
| `avb-message-type` | yes | stable type name |
| `avb-sent-at` | yes | RFC3339 UTC |
| `avb-correlation-id` | no | UUID |
| `avb-causation-id` | no | UUID |
| `avb-tenant-id` | no | tenant identity |
| `avb-reply-to` | no | transport address |
| `avb-request-id` | no | UUID |
| `avb-deadline` | no | RFC3339 UTC |
| `avb-delivery-attempt` | no | positive integer |
| `avb-key-id` | no | signing/encryption key id |
| `avb-signature` | no | base64 signature |
| `avb-claim-check` | no | opaque blob reference |

Transport adapters должны отображать envelope в native properties без изменения семантики. Если broker не поддерживает headers, adapter сериализует полный envelope вместе с body в binary frame.

## 5. JSON body profile

- UTF-8 без BOM.
- camelCase properties.
- даты в RFC3339 с timezone/`Z`.
- enum по умолчанию строкой; numeric enum запрещён для public contracts.
- неизвестные properties игнорируются при backward-compatible режиме.
- `null` и отсутствующее поле не считаются взаимозаменяемыми без schema rule.
- polymorphic CLR type metadata запрещены.

## 6. Correlation and causation

Для входящего `M0`, породившего `M1`:

```text
M1.causation_id = M0.message_id
M1.correlation_id = M0.correlation_id ?? M0.message_id
```

Для root message:

```text
correlation_id = message_id
causation_id = absent
```

## 7. Deadlines and TTL

- `deadline_utc` определяет, имеет ли бизнес-операция смысл.
- Broker TTL определяет срок хранения transport message.
- Adapter выбирает минимальное из deadline и explicit TTL.
- Consumer не начинает handler, если deadline уже истёк.
- Expired message получает terminal outcome `Expired`, а не transient retry.

## 8. Canonical signing representation

Подпись вычисляется не по произвольному JSON envelope, а по фиксированной последовательности UTF-8 полей:

```text
AVB1\n
message_id\n
message_type\n
sent_at_unix_ms\n
correlation_id-or-empty\n
causation_id-or-empty\n
tenant_id-or-empty\n
content_type\n
lowercase-hex-sha256(body)
```

Headers, не перечисленные выше, не входят в signature v1. Изменение canonical representation требует нового `avb-signature-version`.

## 9. Broker mappings

### RabbitMQ

- Queue command: default exchange + queue name routing key.
- Event: topic exchange named by message type.
- `message_id`, `correlation_id`, `reply_to`, `type`, `content_type` используют AMQP properties.
- Остальные поля используют headers.

### Kafka

- Topic определяется route, не обязательно message type.
- `partition_key` становится record key.
- Envelope metadata размещаются в record headers.
- `message_id` не равен Kafka offset.

### InMemory

- Должен сохранять wire semantics, включая serialize/deserialize mode в conformance tests.
- Оптимизация передачи object reference допустима только в отдельном opt-in режиме и не участвует в contract tests.

## 10. Compatibility rules

Backward-compatible:

- добавить optional field;
- расширить допустимый диапазон без изменения wire type;
- добавить новый header;
- добавить новый event type.

Breaking:

- удалить/переименовать required field;
- изменить wire type;
- изменить semantic meaning;
- переиспользовать старое field name для другого смысла;
- сменить command/event semantics без нового major type name.

## 11. Protocol conformance tests

- Round-trip всех обязательных и optional полей.
- Unknown header сохраняется или безопасно игнорируется.
- Unknown message type не вызывает загрузку CLR type.
- Correlation/causation propagation соответствует разделу 6.
- Canonical signature одинакова на всех transports.
- JSON golden files читаются текущей и предыдущей major runtime version.
- Oversized headers отклоняются до broker publish с понятной diagnostic.

## 12. Open questions до Accepted status

1. Максимальный размер header map и каждого значения.
2. Допустимый алфавит tenant id и partition key.
3. Нужен ли `source` как обязательный CloudEvents-compatible URI.
4. Нужна ли structured CloudEvents binding или достаточно binary mapping.
5. Как versioned signature canonicalization взаимодействует с compression и claim-check.