# AvtoBus.Logistics — сэмпл 30 микросервисов (модульный монолит)

Демонстрирует фреймворк AvtoBus на 30 логистических микросервисах в одном процессе
на транспорте `InMemory` (идея 27 — «модульный монолит»).

## Структура

```
AvtoBus.Logistics/
├── AvtoBus.Logistics.slnx          # отдельное решение (не входит в корневой AvtoBus.slnx)
├── Contracts/
│   └── AvtoBus.Logistics.Contracts/  # контракт-пакет без зависимостей (5 неймспейсов)
├── services/                         # 30 сервисов: один сервис — один csproj
│   └── Directory.Build.props         # общая конфигурация (CPM, импорт корневого props)
└── runner/
    └── AvtoBus.Logistics.Runner/     # оркестратор: регистрирует все 30 сервисов и гоняет сквозную цепочку
```

## Сервисы

| Домен | Сервисы |
|---|---|
| Ordering | Orders, Customers, Catalog, Pricing, Inventory, Warehouses |
| Fulfilment | Packing, Parcels, Labels, Shipments, Routing, Dispatch |
| Transport | Fleet, Drivers, Tracking, Geofencing, Delivery, ProofOfDelivery |
| Routing/Hubs | Hubs, Scheduling, Addresses, Customs |
| Finance | Payments, Invoices, Insurance |
| CustomerService | Returns, Claims, Notifications, Ratings, Analytics |

Каждый сервис: `Program.cs` (Host + `AddAvtoBus` + `UseInMemory` + `AddConsumersFromAssembly`) и
`Handlers.cs` (статический класс с методами `Handle(Command, ...)`).

## Демо-сценарии

- Сквозная цепочка: заказ → склад → упаковка → доставка → финансы → клиентский сервис.
- **Доставка с ретрая**: `Delivery.Service` бросает на первой попытке —
  `ImmediateRetries(1)` повторяет, и сообщение доставляется (attempt 2).
- **Платёж отклонён**: суммы кратные 1000 отклоняются `Payments.Service`
  (показано на платеже 1000; основной 84398 проходит).
- **Возврат заказа**: `CancelOrder` → `ReleaseStock` (снятие резерва) → `CompleteReturn` →
  `RefundPayment` — сквозной сценарий по четырём сервисам (Orders, Inventory, Returns, Payments).
- **Операционка**: `SetVehicleOutOfService` (ТО ТС), `RescheduleDelivery` (перенос доставки),
  `RerouteShipment` (перестроение маршрута), `CompleteShift` (завершение смены водителя).
- **Event-driven (publish/subscribe)**: события публикуются в топики и обрабатываются
  подписчиками — `Analytics.Service` слушает `OrderPlaced` (считает метрику заказов),
  `Notifications.Service` слушает `Delivered` (шлёт клиенту уведомление о вручении).

## Команды

| Команда | Сервис | Что происходит |
|---|---|---|
| `PlaceOrder` | Orders | Принимает заказ, публикует `OrderPlaced` |
| `CancelOrder` | Orders | Отменяет заказ, публикует `OrderCancelled` |
| `RegisterCustomer` / `CreateProduct` | Customers / Catalog | Мастер-данные |
| `QuoteShipment` | Pricing | Считает стоимость доставки |
| `ReserveStock` / `ReleaseStock` | Inventory | Резерв / снятие резерва (`StockShortage` для нет SKU) |
| `AllocateWarehouse` / `PackOrder` | Warehouses / Packing | Склад и упаковка |
| `CreateParcel` / `GenerateLabel` | Parcels / Labels | Посылка и наклейка |
| `CreateShipment` / `PlanRoute` | Shipments / Routing | Отправление и маршрут |
| `RerouteShipment` | Routing | Перестроение маршрута (`ShipmentRerouted`) |
| `DispatchShipment` / `AssignDriver` | Dispatch / Drivers | Передача на доставку, водитель |
| `CompleteShift` | Drivers | Завершение смены (`ShiftCompleted`) |
| `RegisterVehicle` / `SetVehicleOutOfService` | Fleet | ТС и вывод на ТО |
| `RecordLocation` / `GeofenceEntered` | Tracking / Geofencing | Позиция и геозоны |
| `AttemptDelivery` / `RescheduleDelivery` | Delivery | Вручение (с ретрая) / перенос окна |
| `CapturePod` / `SortThroughHub` | ProofOfDelivery / Hubs | POD и сорт-хаб |
| `SchedulePickup` / `ValidateAddress` | Scheduling / Addresses | Забор и адрес |
| `ClearCustoms` | Customs | Таможенный выпуск |
| `GenerateInvoice` / `ProcessPayment` | Invoices / Payments | Счёт и оплата |
| `RefundPayment` | Payments | Возврат средств (`PaymentRefunded`) |
| `InsureShipment` / `InitiateReturn` | Insurance / Returns | Страховка и возврат |
| `CompleteReturn` | Returns | Приёмка возврата на складе |
| `FileClaim` / `SendNotification` | Claims / Notifications | Претензия и уведомления |
| `RateDelivery` / `RecordAnalytics` | Ratings / Analytics | Оценка и метрики |

## Запуск

```bash
dotnet build samples/AvtoBus.Logistics/AvtoBus.Logistics.slnx -c Release

# Smoke-прогон: Runner гоняет все 30 сервисов, проверяет сценарии и
# возвращает ненулевой код при провале (используется в CI-джобе logistics-sample).
# Завершается сам: ввода не требует.
dotnet run --project samples/AvtoBus.Logistics/runner/AvtoBus.Logistics.Runner -c Release
```

Ожидаемый финал:

```
=== Проверки сценариев ===
  [OK]   30 сервисов задействованы
  [OK]   Очереди пусты
  [OK]   Доставка с ретрая
  [OK]   Платежи обработаны (1 успех + 1 отклонён)
  [OK]   Заказ отменён / Резерв снят / Возврат завершён / Возврат средств выполнен
  [OK]   ТС выведено на ТО / Доставка перенесена / Маршрут перестроен / Смена завершена
  [OK]   Аналитика слушает OrderPlaced / Уведомления слушают Delivered
  ...  (всего 17 проверок)
Smoke-прогон успешен.
```

## Как вынести сервис в отдельный процесс

При реальном разделении модуля меняется только `runner/Program.cs`:
копия `AddAvtoBus(...)` с хендлерами конкретного сервиса переносится в его `Program.cs`,
транспорт остаётся `InMemory` (в проде — RabbitMQ/Kafka/NATS), а контракты уже изолированы в `Contracts/`.
