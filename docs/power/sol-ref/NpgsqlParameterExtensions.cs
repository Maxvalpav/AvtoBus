using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Persistence.Postgres;

internal static class NpgsqlParameterExtensions
{
    public static void AddNullable(
        this NpgsqlParameterCollection parameters,
        string name,
        NpgsqlDbType type,
        object? value)
    {
        parameters.Add(new NpgsqlParameter(name, type)
        {
            Value = value ?? DBNull.Value
        });
    }
}
