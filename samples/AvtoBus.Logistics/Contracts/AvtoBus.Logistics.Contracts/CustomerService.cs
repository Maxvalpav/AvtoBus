namespace AvtoBus.Logistics.Contracts.CustomerService;

/// <summary>Команда: отправить уведомление. Обрабатывается Notifications.Service.</summary>
public record SendNotification(Guid NotificationId, Guid CustomerId, string Channel, string Template, string Destination) : ICommand;

/// <summary>Событие: уведомление отправлено.</summary>
public record NotificationSent(Guid NotificationId, Guid CustomerId, string Channel, string ExternalMessageId) : IEvent;

/// <summary>Команда: оценить доставку. Обрабатывается Ratings.Service.</summary>
public record RateDelivery(Guid RatingId, Guid ShipmentId, int Score, string? Comment) : ICommand;

/// <summary>Событие: оценка сохранена.</summary>
public record DeliveryRated(Guid RatingId, Guid ShipmentId, int Score) : IEvent;

/// <summary>Команда: зафиксировать аналитическое событие. Обрабатывается Analytics.Service.</summary>
public record RecordAnalytics(Guid EventId, string Metric, double Value, string Dimension) : ICommand;

/// <summary>Событие: аналитическое событие записано.</summary>
public record AnalyticsRecorded(Guid EventId, string Metric, double Value) : IEvent;