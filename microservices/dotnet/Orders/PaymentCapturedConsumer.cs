using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

namespace Orders;

/// <summary>
/// Confirms orders when payment is captured. Idempotent: a redelivered event changes
/// nothing. The offset is committed only after the database transaction succeeds.
/// </summary>
public class PaymentCapturedConsumer(NpgsqlDataSource db, ILogger<PaymentCapturedConsumer> log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKERS"),
            GroupId = "orders",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe("payments.payment-captured");

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string> result;
            try { result = consumer.Consume(stoppingToken); }
            catch (OperationCanceledException) { break; }

            while (true) // retry until the database accepts it
            {
                try { await Confirm(result.Message.Value, stoppingToken); break; }
                catch (Exception e) when (!stoppingToken.IsCancellationRequested)
                {
                    log.LogError(e, "processing failed, retrying");
                    await Task.Delay(1000, stoppingToken);
                }
            }
            consumer.Commit(result);
        }
        consumer.Close();
    }, stoppingToken);

    private async Task Confirm(string payload, CancellationToken ct)
    {
        string? eventId, orderId;
        try
        {
            using var doc = JsonDocument.Parse(payload);
            eventId = doc.RootElement.GetProperty("eventId").GetString();
            orderId = doc.RootElement.GetProperty("orderId").GetString();
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException)
        {
            log.LogError("skipping malformed event");
            return;
        }
        if (!Guid.TryParse(eventId, out var eventGuid)) return;

        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using var insert = new NpgsqlCommand(
            "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING", conn, tx);
        insert.Parameters.AddWithValue(eventGuid);
        if (await insert.ExecuteNonQueryAsync(ct) == 1)
        {
            await using var update = new NpgsqlCommand(
                "UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id::text = $1", conn, tx);
            update.Parameters.AddWithValue(orderId!);
            await update.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
    }
}
