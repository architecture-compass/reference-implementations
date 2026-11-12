using Confluent.Kafka;
using Npgsql;

namespace Payments;

public static class Db
{
    /// <summary>Converts postgres://user:pass@host:port/db into an Npgsql connection string.</summary>
    public static string ConnectionString(string url)
    {
        var uri = new Uri(url);
        var credentials = uri.UserInfo.Split(':', 2);
        return new NpgsqlConnectionStringBuilder
        {
            Host = uri.Host,
            Port = uri.Port,
            Database = uri.AbsolutePath.TrimStart('/'),
            Username = Uri.UnescapeDataString(credentials[0]),
            Password = Uri.UnescapeDataString(credentials[1]),
            MaxPoolSize = 10,
        }.ConnectionString;
    }
}

public static class Outbox
{
    public static async Task Insert(NpgsqlConnection conn, NpgsqlTransaction tx, string aggregateType,
        string aggregateId, string eventType, string topic, string payload)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """, conn, tx);
        cmd.Parameters.AddWithValue(Guid.NewGuid());
        cmd.Parameters.AddWithValue(aggregateType);
        cmd.Parameters.AddWithValue(aggregateId);
        cmd.Parameters.AddWithValue(eventType);
        cmd.Parameters.AddWithValue(topic);
        cmd.Parameters.AddWithValue(payload);
        await cmd.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// Every 500 ms: lock unpublished outbox rows, send them to Kafka, mark them published.
/// If Kafka fails, the transaction rolls back and the rows are retried.
/// </summary>
public class OutboxRelay(NpgsqlDataSource db, ILogger<OutboxRelay> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKERS"),
            Acks = Acks.All,
            EnableIdempotence = true,
        }).Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try { await RelayOnce(producer, stoppingToken); }
            catch (Exception e) when (!stoppingToken.IsCancellationRequested)
            {
                log.LogError(e, "outbox relay failed; rows stay unpublished and are retried");
            }
            await Task.Delay(500, stoppingToken);
        }
    }

    private async Task RelayOnce(IProducer<string, string> producer, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var rows = new List<(Guid Id, string Topic, string Key, string Payload)>();
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT id, topic, aggregate_id, payload::text FROM outbox
            WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
            """, conn, tx))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                rows.Add((reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }
        if (rows.Count == 0) return;

        foreach (var row in rows)
            await producer.ProduceAsync(row.Topic, new Message<string, string> { Key = row.Key, Value = row.Payload }, ct);

        await using (var update = new NpgsqlCommand("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", conn, tx))
        {
            update.Parameters.AddWithValue(rows.Select(r => r.Id).ToArray());
            await update.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
    }
}
