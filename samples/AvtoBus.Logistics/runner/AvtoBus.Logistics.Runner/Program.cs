using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Logistics.Contracts.CustomerService;
using AvtoBus.Logistics.Contracts.Finance;
using AvtoBus.Logistics.Contracts.Fulfilment;
using AvtoBus.Logistics.Contracts.Ordering;
using AvtoBus.Logistics.Contracts.Transport;
using AvtoBus.Observability;
using Logistics.Addresses.Handlers;
using Logistics.Analytics.Handlers;
using Logistics.Catalog.Handlers;
using Logistics.Claims.Handlers;
using Logistics.Customers.Handlers;
using Logistics.Customs.Handlers;
using Logistics.Delivery.Handlers;
using Logistics.Dispatch.Handlers;
using Logistics.Drivers.Handlers;
using Logistics.Fleet.Handlers;
using Logistics.Geofencing.Handlers;
using Logistics.Hubs.Handlers;
using Logistics.Insurance.Handlers;
using Logistics.Inventory.Handlers;
using Logistics.Invoices.Handlers;
using Logistics.Labels.Handlers;
using Logistics.Notifications.Handlers;
using Logistics.Orders.Handlers;
using Logistics.Packing.Handlers;
using Logistics.Parcels.Handlers;
using Logistics.Payments.Handlers;
using Logistics.Pricing.Handlers;
using Logistics.ProofOfDelivery.Handlers;
using Logistics.Ratings.Handlers;
using Logistics.Returns.Handlers;
using Logistics.Routing.Handlers;
using Logistics.Scheduling.Handlers;
using Logistics.Shipments.Handlers;
using Logistics.Tracking.Handlers;
using Logistics.Warehouses.Handlers;
using Microsoft.Extensions.DependencyInjection;

// Модульный монолит (идея 27): все 30 сервисов живут в одном процессе на InMemory.
// При выносе модуля в отдельный сервис меняется только этот файл конфигурации.
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseInMemory();
    bus.ServiceName("logistics-runner");
    bus.Recoverability(r => r
        .ImmediateRetries(1)
        .DelayedRetries(3, Backoff.Exponential(TimeSpan.FromSeconds(1))));

    // Регистрация хендлеров всех 30 сервисов в одном процессе.
    bus.AddConsumersFromAssembly(typeof(OrderHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(CustomerHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(CatalogHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(InventoryHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(WarehouseHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(PricingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(PaymentHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(InvoiceHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(PackingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(ParcelHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(LabelHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(ShipmentHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(RoutingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(DispatchHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(FleetHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(DriverHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(TrackingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(GeofencingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(DeliveryHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(PodHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(HubHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(SchedulingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(AddressHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(CustomsHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(InsuranceHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(ReturnHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(ClaimHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(NotificationHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(RatingHandlers).Assembly);
    bus.AddConsumersFromAssembly(typeof(AnalyticsHandlers).Assembly);
});

using var host = builder.Build();
await host.StartAsync();

Console.WriteLine("=== AvtoBus Logistics: 30 сервисов в одном процессе (InMemory) ===\n");

var bus = host.Services.GetRequiredService<IBus>();
var correlation = Guid.NewGuid();
var orderId = Guid.NewGuid();
var customerId = Guid.NewGuid();
var sku = Guid.NewGuid();
var shipmentId = Guid.NewGuid();
var parcelId = Guid.NewGuid();
var vehicleId = Guid.NewGuid();
var driverId = Guid.NewGuid();

// ---- Сквозная цепочка: заказ → склад → упаковка → доставка → финансы → клиентский сервис.

await bus.SendAsync(new RegisterCustomer(customerId, "Анна Иванова", "anna@example.com", "+7 900 000-00-00"));
await bus.SendAsync(new CreateProduct(sku, "Кресло Eames", "Мебель", 42199m, 12000));
await bus.SendAsync(new PlaceOrder(orderId, customerId,
    [new OrderLine(sku, 2, 42199m)],
    "Москва, Ленинский пр-т, 30", correlation));

await bus.SendAsync(new QuoteShipment(Guid.NewGuid(), orderId, "Москва", "Санкт-Петербург", 24000, "standard"));
await bus.SendAsync(new ReserveStock(orderId, [new ReserveLine(sku, 2)]));
await bus.SendAsync(new AllocateWarehouse(orderId, "central"));
await bus.SendAsync(new PackOrder(orderId, "WH-01"));

await bus.SendAsync(new CreateParcel(orderId, "PKG-001", 24000));
await bus.SendAsync(new GenerateLabel(parcelId, "Санкт-Петербург, Невский пр-т, 10"));
await bus.SendAsync(new CreateShipment(shipmentId, orderId, parcelId, "Москва", "Санкт-Петербург"));

await bus.SendAsync(new PlanRoute(shipmentId, "Москва", "Санкт-Петербург", ["Тверь", "Великий Новгород"]));
await bus.SendAsync(new RegisterVehicle(vehicleId, "А123ВС777", "gazelle", 1500));
await bus.SendAsync(new AssignDriver(driverId, vehicleId.ToString(), "R-8821"));
await bus.SendAsync(new DispatchShipment(shipmentId, vehicleId.ToString(), driverId.ToString()));

await bus.SendAsync(new RecordLocation(shipmentId, 55.7558, 37.6173, DateTimeOffset.UtcNow));
await bus.SendAsync(new GeofenceEntered(shipmentId, "Z-MSK-CENTER", DateTimeOffset.UtcNow));

// Delivery бросает на первой попытке — ImmediateRetries(1) повторит и доставит.
await bus.SendAsync(new AttemptDelivery(shipmentId, DateTimeOffset.UtcNow.AddHours(8)));
await bus.SendAsync(new CapturePod(shipmentId, "Иванова А.", DateTimeOffset.UtcNow.AddHours(8)));

await bus.SendAsync(new SortThroughHub(shipmentId, "HUB-SPB-1"));
await bus.SendAsync(new SchedulePickup(shipmentId, "Москва", DateTimeOffset.UtcNow.AddHours(1), DateTimeOffset.UtcNow.AddHours(4)));
await bus.SendAsync(new ValidateAddress(Guid.NewGuid(), "Санкт-Петербург, Невский пр-т, 10"));
await bus.SendAsync(new ClearCustoms(shipmentId, "DCL-2026-0000001"));

await bus.SendAsync(new GenerateInvoice(Guid.NewGuid(), orderId, customerId, 85000m));
await bus.SendAsync(new ProcessPayment(Guid.NewGuid(), orderId, 84398m, "card"));
await bus.SendAsync(new ProcessPayment(Guid.NewGuid(), orderId, 1000m, "card"));
await bus.SendAsync(new InsureShipment(shipmentId, 100000m));

await bus.SendAsync(new SendNotification(Guid.NewGuid(), customerId, "email", "delivery_eta", "anna@example.com"));
await bus.SendAsync(new RateDelivery(Guid.NewGuid(), shipmentId, 5, "Быстро и аккуратно"));
await bus.SendAsync(new RecordAnalytics(Guid.NewGuid(), "orders.created", 1, "ecommerce"));

// Возврат и претензия — обрабатываются асинхронно своими сервисами.
var returnId = Guid.NewGuid();
await bus.SendAsync(new InitiateReturn(returnId, orderId, "не подошёл размер"));
await bus.SendAsync(new FileClaim(Guid.NewGuid(), shipmentId, "повреждение", 5000m));

// ---- Сценарий «возврат заказа»: отмена → снятие резерва → завершение возврата → рефанд.
await bus.SendAsync(new CancelOrder(orderId, customerId, "клиент отказался от заказа", correlation));
await bus.SendAsync(new ReleaseStock(orderId, [new ReserveLine(sku, 2)]));
await bus.SendAsync(new CompleteReturn(returnId, orderId, $"RMA-{returnId:N}", DateTimeOffset.UtcNow.AddDays(1)));
await bus.SendAsync(new RefundPayment(Guid.NewGuid(), orderId, 84398m, "return_completed"));

// ---- Операционный сценарий: ТО ТС, перенос доставки, перемаршрутизация, завершение смены.
await bus.SendAsync(new SetVehicleOutOfService(vehicleId, "плановое ТО"));
await bus.SendAsync(new RescheduleDelivery(shipmentId, DateTimeOffset.UtcNow.AddDays(1).AddHours(10), "получатель отсутствовал"));
await bus.SendAsync(new RerouteShipment(shipmentId, "пробки на М-11", ["Тверь", "Валдай"]));
await bus.SendAsync(new CompleteShift(driverId, "R-8821", DateTimeOffset.UtcNow.AddHours(9)));

// ---- Ждём, пока все очереди опустеют и консьюмеры обработают сообщения.
await WaitForIdleAsync(host.Services, TimeSpan.FromSeconds(30));

Console.WriteLine("\n=== Итоги прогона ===");

var consumer = host.Services.GetRequiredService<AvtoBus.Runtime.ConsumerHost>();
var summary = consumer.Runners
    .OrderBy(r => r.Name)
    .Select(r => new { r.Name, r.Processed, r.Failed })
    .ToArray();

foreach (var row in summary)
    Console.WriteLine($"  {row.Name,-40} processed: {row.Processed,3}  failed: {row.Failed}");

var transport = host.Services.GetRequiredService<InMemoryTransport>();
var leftover = transport.QueueDepths
    .Where(kvp => kvp.Value > 0)
    .OrderBy(kvp => kvp.Key)
    .ToArray();
foreach (var (queue, depth) in leftover)
    Console.WriteLine($"  [ОСТАЛОСЬ] {queue}: {depth}");

var checks = new List<(string Name, bool Ok, string Detail)>
{
    ("30 сервисов задействованы", summary.Count(r => r.Processed > 0) >= 30, $"active={summary.Count(r => r.Processed > 0)}/{summary.Length}"),
    ("Очереди пусты", leftover.Length == 0, $"leftover={leftover.Length}"),
};

// Ожидаемые демо-сценарии.
var delivery = summary.FirstOrDefault(r => r.Name == "attempt-delivery");
checks.Add(("Доставка с ретрая", delivery?.Processed >= 1 && delivery.Failed >= 1, $"processed={delivery?.Processed} failed={delivery?.Failed}"));

var payment = summary.FirstOrDefault(r => r.Name == "process-payment");
checks.Add(("Платежи обработаны (1 успех + 1 отклонён)", payment?.Processed == 2, $"processed={payment?.Processed}"));

var invoice = summary.FirstOrDefault(r => r.Name == "generate-invoice");
checks.Add(("Счёт выставлен", invoice?.Processed >= 1, $"processed={invoice?.Processed}"));

var analytics = summary.FirstOrDefault(r => r.Name == "record-analytics");
checks.Add(("Аналитика записана", analytics?.Processed >= 1, $"processed={analytics?.Processed}"));

var claim = summary.FirstOrDefault(r => r.Name == "file-claim");
checks.Add(("Претензия принята", claim?.Processed >= 1, $"processed={claim?.Processed}"));

// Новые сценарии: возврат заказа и операционка.
var cancel = summary.FirstOrDefault(r => r.Name == "cancel-order");
checks.Add(("Заказ отменён", cancel?.Processed >= 1, $"processed={cancel?.Processed}"));

var release = summary.FirstOrDefault(r => r.Name == "release-stock");
checks.Add(("Резерв снят (возврат)", release?.Processed >= 1, $"processed={release?.Processed}"));

var returnDone = summary.FirstOrDefault(r => r.Name == "complete-return");
checks.Add(("Возврат завершён", returnDone?.Processed >= 1, $"processed={returnDone?.Processed}"));

var refund = summary.FirstOrDefault(r => r.Name == "refund-payment");
checks.Add(("Возврат средств выполнен", refund?.Processed >= 1, $"processed={refund?.Processed}"));

var vehicle = summary.FirstOrDefault(r => r.Name == "set-vehicle-out-of-service");
checks.Add(("ТС выведено на ТО", vehicle?.Processed >= 1, $"processed={vehicle?.Processed}"));

var resched = summary.FirstOrDefault(r => r.Name == "reschedule-delivery");
checks.Add(("Доставка перенесена", resched?.Processed >= 1, $"processed={resched?.Processed}"));

var reroute = summary.FirstOrDefault(r => r.Name == "reroute-shipment");
checks.Add(("Маршрут перестроен", reroute?.Processed >= 1, $"processed={reroute?.Processed}"));

var shift = summary.FirstOrDefault(r => r.Name == "complete-shift");
checks.Add(("Смена водителя завершена", shift?.Processed >= 1, $"processed={shift?.Processed}"));

// Event-driven подписки (publish/subscribe): Analytics слушает OrderPlaced, Notifications — Delivered.
var orderTopic = summary.FirstOrDefault(r => r.Name == "ordering.order-placed");
checks.Add(("Аналитика слушает OrderPlaced", orderTopic?.Processed >= 1, $"processed={orderTopic?.Processed}"));

var deliveredTopic = summary.FirstOrDefault(r => r.Name == "transport.delivered");
checks.Add(("Уведомления слушают Delivered", deliveredTopic?.Processed >= 1, $"processed={deliveredTopic?.Processed}"));

Console.WriteLine("\n=== Проверки сценариев ===");
var failed = 0;
foreach (var (name, ok, detail) in checks)
{
    Console.WriteLine($"  {(ok ? "[OK]  " : "[FAIL]")} {name,-32} ({detail})");
    if (!ok)
        failed++;
}

await host.StopAsync();

Console.WriteLine(failed == 0
    ? "\nSmoke-прогон успешен."
    : $"\nSmoke-прогон: {failed} проверк(и) не прошли.");

return failed;

static async Task WaitForIdleAsync(IServiceProvider services, TimeSpan timeout)
{
    var queues = services.GetService<IQueueDepthProvider>();
    var lag = services.GetService<IConsumerLagProvider>();
    var consumer = services.GetRequiredService<AvtoBus.Runtime.ConsumerHost>();

    var deadline = DateTimeOffset.UtcNow + timeout;
    while (DateTimeOffset.UtcNow < deadline)
    {
        var pendingQueues = queues?.QueueDepths.Values.Sum() ?? 0;
        var pendingLags = lag?.ConsumerLags.Values.Sum() ?? 0;
        var inFlight = consumer.Runners.Sum(r => r.Lag);

        if (pendingQueues == 0 && pendingLags == 0 && inFlight == 0)
            return;

        await Task.Delay(100);
    }

    Console.WriteLine("[warn] Таймаут ожидания обработки сообщений");
}