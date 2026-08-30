# ADR-0004: Handler contract, dispatch и каскадный return

- Статус: Proposed
- Дата: 2026
- Область: public API, source generation

## Контекст

AvtoBus планирует поддерживать method handlers и `IConsumer<T>`. Без строгого контракта непонятно, какие параметры являются сообщением, как разрешаются зависимости и куда отправляется возвращённый объект.

## Решение

### Method handler

Handler удовлетворяет правилам:

- имя `Handle`;
- первый параметр - message contract;
- остальные параметры разрешаются из scoped DI;
- `CancellationToken` разрешается из consume context;
- одновременно может существовать ровно один command handler;
- event handlers могут быть множественными;
- generic/open handlers не входят в MVP.

```csharp
public static Task<OrderPlaced> Handle(
    PlaceOrder command,
    OrdersDbContext db,
    CancellationToken ct);
```

### Return semantics

| Return type | Поведение |
|---|---|
| `void`, `Task`, `ValueTask` | Нет исходящего сообщения |
| `T : IEvent` | Publish через текущий `IMessageSession` |
| `T : ICommand` | Send через текущий `IMessageSession` |
| `Result<T>` | Business outcome + optional outgoing message |
| `OutgoingMessages` | Явный набор send/publish/schedule |
| Tuple | Не входит в MVP; неоднозначен и плохо эволюционирует |

Неизвестный return type является compile-time diagnostic, а не автоматическим Publish.

### Dispatch

- Source Generator создаёт typed delegate без `MethodInfo.Invoke`.
- Reflection fallback разрешён только для tests/development и должен быть отключаемым.
- Generated dispatcher не создаёт дополнительный DI scope; scope создаёт host ровно один раз на delivery attempt.
- Handler class lifetime определяется DI registration; instance handler регистрируется scoped.

## Диагностики

| Код | Severity | Условие |
|---|---|---|
| AVB001 | Error | Command не имеет handler |
| AVB002 | Error | Command имеет больше одного handler |
| AVB004 | Error | Первый параметр не является message contract |
| AVB005 | Error | Unsupported return type |
| AVB006 | Warning | Handler class не зарегистрирован в DI |
| AVB007 | Warning | Event не имеет subscriber в текущем solution graph |

AVB007 не может быть Error: подписчик может находиться в другом repository/deployment.

## Последствия

- API предсказуем и анализируется на compile time.
- Tuple cascade отложен до появления убедительного use case.
- `OutgoingMessages` остаётся escape hatch для динамической маршрутизации.

## Проверка решения

- Snapshot tests сгенерированного кода.
- Compile tests для каждой диагностики.
- Runtime parity tests: generated и reflection fallback дают одинаковый результат.
- AOT test подтверждает отсутствие reflection warnings на generated path.