using System.Threading.Channels;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace AvtoBus.Runtime;

/// <param name="MessageType">Тип сообщения или <c>null</c> для служебных очередей (reply).</param>
public sealed record ConsumerSubscription(
    ITransport Transport,
    TransportSubscription Subscription,
    Type? MessageType,
    ConsumerSettings? Settings = null);

