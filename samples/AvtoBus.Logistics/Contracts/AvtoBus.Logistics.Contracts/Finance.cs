namespace AvtoBus.Logistics.Contracts.Finance;

/// <summary>Команда: списать оплату. Обрабатывается Payments.Service.</summary>
public record ProcessPayment(Guid PaymentId, Guid OrderId, decimal Amount, string Method) : ICommand;

/// <summary>Событие: оплата прошла.</summary>
public record PaymentSucceeded(Guid PaymentId, Guid OrderId, decimal Amount) : IEvent;

/// <summary>Событие: оплата отклонена.</summary>
public record PaymentFailed(Guid PaymentId, Guid OrderId, decimal Amount, string Reason) : IEvent;

/// <summary>Команда: выставить счёт. Обрабатывается Invoices.Service.</summary>
public record GenerateInvoice(Guid InvoiceId, Guid OrderId, Guid CustomerId, decimal Amount) : ICommand;

/// <summary>Событие: счёт выставлен.</summary>
public record InvoiceGenerated(Guid InvoiceId, Guid OrderId, string InvoiceNumber, decimal Amount) : IEvent;

/// <summary>Команда: застраховать отправление. Обрабатывается Insurance.Service.</summary>
public record InsureShipment(Guid ShipmentId, decimal InsuredValue) : ICommand;

/// <summary>Событие: отправление застраховано.</summary>
public record ShipmentInsured(Guid ShipmentId, string PolicyNumber, decimal InsuredValue) : IEvent;

/// <summary>Команда: оформить возврат. Обрабатывается Returns.Service.</summary>
public record InitiateReturn(Guid ReturnId, Guid OrderId, string Reason) : ICommand;

/// <summary>Событие: возврат оформлен.</summary>
public record ReturnInitiated(Guid ReturnId, Guid OrderId, string RmaNumber) : IEvent;

/// <summary>Команда: завершить возврат (товар принят на складе). Обрабатывается Returns.Service.</summary>
public record CompleteReturn(Guid ReturnId, Guid OrderId, string RmaNumber, DateTimeOffset CompletedAt) : ICommand;

/// <summary>Событие: возврат завершён.</summary>
public record ReturnCompleted(Guid ReturnId, Guid OrderId, string RmaNumber) : IEvent;

/// <summary>Команда: вернуть деньги за заказ. Обрабатывается Payments.Service.</summary>
public record RefundPayment(Guid RefundId, Guid OrderId, decimal Amount, string Reason) : ICommand;

/// <summary>Событие: возврат средств выполнен.</summary>
public record PaymentRefunded(Guid RefundId, Guid OrderId, decimal Amount) : IEvent;

/// <summary>Команда: зарегистрировать претензию. Обрабатывается Claims.Service.</summary>
public record FileClaim(Guid ClaimId, Guid ShipmentId, string Reason, decimal Amount) : ICommand;

/// <summary>Событие: претензия зарегистрирована.</summary>
public record ClaimFiled(Guid ClaimId, Guid ShipmentId, string ClaimNumber) : IEvent;