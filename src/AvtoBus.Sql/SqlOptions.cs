namespace AvtoBus.Sql;

/// <summary>
/// Настройки SQL-транспорта (идеи 66–67): PostgreSQL таблица-очередь с
/// FOR UPDATE SKIP LOCKED + LISTEN/NOTIFY для мгновенного пробуждения.
/// </summary>
public sealed class SqlOptions
{
    /// <summary>Строка подключения PostgreSQL.</summary>
    public string ConnectionString { get; set; } = "Host=localhost;Database=avtobus;Username=postgres;Password=postgres";

    /// <summary>Имя приложения — суффикс имён консьюмеров (claimed_by).</summary>
    public string Name { get; set; } = "avtobus";

    /// <summary>Размер batch-а выборки за один FOR UPDATE SKIP LOCKED.</summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Сколько сообщений считается «зависшими»: консьюмер, который не подтвердил
    /// дольше этого времени, теряет claim — сообщение возвращается в доставку (идея 66).
    /// </summary>
    public TimeSpan ReclaimTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Максимальное время ожидания NOTIFY перед очередным опросом.</summary>
    public TimeSpan ListenTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Префикс таблиц-очередей в схеме БД.</summary>
    public string TablePrefix { get; set; } = "avtobus_";
}
