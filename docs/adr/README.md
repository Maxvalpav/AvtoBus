# Architecture Decision Records

> **Статус: Reference.** ADR фиксирует одно решение, его контекст, последствия и проверку. `Proposed` не является окончательным решением до реализации spike и review.

## Индекс

| ADR | Статус | Решение |
|---|---|---|
| [0001](./0001-core-boundaries.md) | Proposed | Границы `AvtoBus.Core` и dependency graph |
| [0002](./0002-bus-lifetime-and-uow.md) | Proposed | Singleton `IBus`, scoped `IMessageSession`, transaction boundary |
| [0003](./0003-delivery-semantics.md) | Proposed | At-least-once и effectively-once terminology |
| [0004](./0004-handler-contract.md) | Proposed | Handler signatures, generated dispatch, cascade semantics |
| [0005](./0005-request-reply.md) | Proposed | Reply endpoint и waiter lifecycle |

## Lifecycle

```text
Proposed -> Accepted -> Superseded
                  \-> Deprecated
Proposed -> Rejected
```

`Accepted` требует:

1. согласованного public API;
2. spike или компилируемого prototype;
3. ссылок на verification matrix;
4. отсутствия нерешённого противоречия с другим Accepted ADR.

## Именование

Файл: `NNNN-short-kebab-title.md`.

ADR не редактируется задним числом после `Accepted`, кроме исправления опечаток. Новое решение создаёт новый ADR и помечает старый `Superseded by ADR-NNNN`.