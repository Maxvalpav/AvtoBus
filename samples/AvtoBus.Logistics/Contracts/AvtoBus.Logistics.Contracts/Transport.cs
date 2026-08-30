namespace AvtoBus.Logistics.Contracts.Transport;

/// <summary>Команда: создать отправление. Обрабатывается Shipments.Service.</summary>
public record CreateShipment(
    Guid ShipmentId,
    Guid OrderId,
    Guid ParcelId,
    string Origin,
    string Destination) : ICommand;

/// <summary>Событие: отправление создано.</summary>
public record ShipmentCreated(Guid ShipmentId, Guid OrderId, Guid ParcelId) : IEvent;

/// <summary>Команда: построить маршрут. Обрабатывается Routing.Service.</summary>
public record PlanRoute(Guid ShipmentId, string Origin, string Destination, IReadOnlyList<string> Waypoints) : ICommand;

/// <summary>Событие: маршрут построен.</summary>
public record RoutePlanned(Guid ShipmentId, string RouteId, int EstimatedMinutes) : IEvent;

/// <summary>Команда: перестроить маршрут (задержка, пробки, новый клиент). Обрабатывается Routing.Service.</summary>
public record RerouteShipment(Guid ShipmentId, string Reason, IReadOnlyList<string> NewWaypoints) : ICommand;

/// <summary>Событие: маршрут перестроен.</summary>
public record ShipmentRerouted(Guid ShipmentId, string RouteId, int NewEstimatedMinutes, string Reason) : IEvent;

/// <summary>Команда: назначить доставку на транспортное средство. Обрабатывается Dispatch.Service.</summary>
public record DispatchShipment(Guid ShipmentId, string VehicleId, string DriverId) : ICommand;

/// <summary>Событие: отправление передано на доставку.</summary>
public record ShipmentDispatched(Guid ShipmentId, string VehicleId, string DriverId) : IEvent;

/// <summary>Команда: зарегистрировать транспортное средство. Обрабатывается Fleet.Service.</summary>
public record RegisterVehicle(Guid VehicleId, string RegistrationPlate, string Type, int CapacityKg) : ICommand;

/// <summary>Событие: транспортное средство зарегистрировано.</summary>
public record VehicleRegistered(Guid VehicleId, string RegistrationPlate, string Type) : IEvent;

/// <summary>Команда: вывести ТС из эксплуатации (ТО, ремонт). Обрабатывается Fleet.Service.</summary>
public record SetVehicleOutOfService(Guid VehicleId, string Reason) : ICommand;

/// <summary>Событие: ТС выведено из эксплуатации.</summary>
public record VehicleOutOfService(Guid VehicleId, string Reason) : IEvent;

/// <summary>Команда: назначить водителя на рейс. Обрабатывается Drivers.Service.</summary>
public record AssignDriver(Guid DriverId, string VehicleId, string RouteId) : ICommand;

/// <summary>Событие: водитель назначен.</summary>
public record DriverAssigned(Guid DriverId, string VehicleId, string RouteId) : IEvent;

/// <summary>Команда: завершить смену водителя. Обрабатывается Drivers.Service.</summary>
public record CompleteShift(Guid DriverId, string RouteId, DateTimeOffset EndedAt) : ICommand;

/// <summary>Событие: смена водителя завершена.</summary>
public record ShiftCompleted(Guid DriverId, string RouteId, DateTimeOffset EndedAt) : IEvent;

/// <summary>Команда: записать текущее местоположение. Обрабатывается Tracking.Service.</summary>
public record RecordLocation(Guid ShipmentId, double Latitude, double Longitude, DateTimeOffset At) : ICommand;

/// <summary>Событие: местоположение записано.</summary>
public record LocationRecorded(Guid ShipmentId, double Latitude, double Longitude) : IEvent;

/// <summary>Команда: зафиксировать пересечение геозоны. Обрабатывается Geofencing.Service.</summary>
public record GeofenceEntered(Guid ShipmentId, string ZoneId, DateTimeOffset At) : ICommand;

/// <summary>Событие: пересечение геозоны зафиксировано.</summary>
public record GeofenceCrossed(Guid ShipmentId, string ZoneId, string Direction) : IEvent;

/// <summary>Команда: попытаться вручить доставку. Обрабатывается Delivery.Service.</summary>
public record AttemptDelivery(Guid ShipmentId, DateTimeOffset At) : ICommand;

/// <summary>Событие: доставка вручена.</summary>
public record Delivered(Guid ShipmentId, DateTimeOffset At) : IEvent;

/// <summary>Событие: доставка не удалась.</summary>
public record DeliveryFailed(Guid ShipmentId, string Reason, int Attempt) : IEvent;

/// <summary>Команда: перенести доставку на новое окно. Обрабатывается Delivery.Service.</summary>
public record RescheduleDelivery(Guid ShipmentId, DateTimeOffset NewWindowStart, string Reason) : ICommand;

/// <summary>Событие: доставка перенесена.</summary>
public record DeliveryRescheduled(Guid ShipmentId, DateTimeOffset NewWindowStart, string Reason) : IEvent;

/// <summary>Команда: получить подтверждение доставки (POD). Обрабатывается ProofOfDelivery.Service.</summary>
public record CapturePod(Guid ShipmentId, string RecipientSignature, DateTimeOffset At) : ICommand;

/// <summary>Событие: подтверждение получено.</summary>
public record PodCaptured(Guid ShipmentId, string RecipientSignature) : IEvent;

/// <summary>Команда: пропустить посылку через сортировочный хаб. Обрабатывается Hubs.Service.</summary>
public record SortThroughHub(Guid ShipmentId, string HubId) : ICommand;

/// <summary>Событие: посылка обработана хабом.</summary>
public record HubProcessed(Guid ShipmentId, string HubId, string DestinationBay) : IEvent;

/// <summary>Команда: запланировать забор груза. Обрабатывается Scheduling.Service.</summary>
public record SchedulePickup(Guid ShipmentId, string Origin, DateTimeOffset WindowStart, DateTimeOffset WindowEnd) : ICommand;

/// <summary>Событие: забор запланирован.</summary>
public record PickupScheduled(Guid ShipmentId, string PickupSlotId) : IEvent;

/// <summary>Команда: проверить адрес. Обрабатывается Addresses.Service.</summary>
public record ValidateAddress(Guid AddressId, string RawAddress) : ICommand;

/// <summary>Событие: адрес проверен и нормализован.</summary>
public record AddressValidated(Guid AddressId, string NormalizedAddress, bool IsDeliverable) : IEvent;

/// <summary>Команда: выпустить груз на таможне. Обрабатывается Customs.Service.</summary>
public record ClearCustoms(Guid ShipmentId, string DeclarationNumber) : ICommand;

/// <summary>Событие: таможенный выпуск получен.</summary>
public record CustomsCleared(Guid ShipmentId, string ClearanceCode) : IEvent;