namespace AvtoBus.Logistics.Contracts.Fulfilment;

/// <summary>Команда: зарезервировать товар на складе. Обрабатывается Inventory.Service.</summary>
public record ReserveStock(Guid OrderId, IReadOnlyList<ReserveLine> Lines) : ICommand;

public record ReserveLine(Guid Sku, int Quantity);

/// <summary>Событие: резерв успешен.</summary>
public record StockReserved(Guid OrderId) : IEvent;

/// <summary>Событие: резерв невозможен, не хватает остатков.</summary>
public record StockShortage(Guid OrderId, Guid Sku, int Requested, int Available) : IEvent;

/// <summary>Команда: снять резерв товара (при возврате/отмене). Обрабатывается Inventory.Service.</summary>
public record ReleaseStock(Guid OrderId, IReadOnlyList<ReserveLine> Lines) : ICommand;

/// <summary>Событие: резерв снят, товар снова доступен.</summary>
public record StockReleased(Guid OrderId, Guid Sku, int Quantity) : IEvent;

/// <summary>Команда: выделить склад под заказ. Обрабатывается Warehouses.Service.</summary>
public record AllocateWarehouse(Guid OrderId, string Region) : ICommand;

/// <summary>Событие: склад выделен.</summary>
public record WarehouseAllocated(Guid OrderId, string WarehouseId) : IEvent;

/// <summary>Команда: упаковать заказ. Обрабатывается Packing.Service.</summary>
public record PackOrder(Guid OrderId, string WarehouseId) : ICommand;

/// <summary>Событие: заказ упакован.</summary>
public record OrderPacked(Guid OrderId, string PackageId) : IEvent;

/// <summary>Команда: создать посылку. Обрабатывается Parcels.Service.</summary>
public record CreateParcel(Guid OrderId, string PackageId, int WeightGrams) : ICommand;

/// <summary>Событие: посылка создана и взвешена.</summary>
public record ParcelCreated(Guid OrderId, string ParcelId, int WeightGrams) : IEvent;

/// <summary>Команда: сгенерировать транспортную наклейку. Обрабатывается Labels.Service.</summary>
public record GenerateLabel(Guid ParcelId, string Destination) : ICommand;

/// <summary>Событие: наклейка сгенерирована.</summary>
public record LabelGenerated(Guid ParcelId, string LabelUrl) : IEvent;