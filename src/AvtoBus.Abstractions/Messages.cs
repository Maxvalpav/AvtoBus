namespace AvtoBus.Abstractions;

public interface IAvtoMessage
{
    static abstract string SchemaName { get; }
    static abstract int SchemaVersion { get; }
}

public interface ICommand : IAvtoMessage;

public interface ICommand<TReply> : ICommand;

public interface IEvent : IAvtoMessage;

public interface IQuery<TReply> : IAvtoMessage;

public interface IPartitionedMessage
{
    string PartitionKey { get; }
}
