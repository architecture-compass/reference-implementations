using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

namespace Payments;

/// <summary>
/// Captures the payment and records the event in one transaction. A redelivered event,
/// or a second event for the same order, changes nothing. The offset is committed only
/// after the transaction succeeds.
/// </summary>
public class OrderPlacedConsumer(NpgsqlDataSource db, ILogger<OrderPlacedConsumer> log) : BackgroundService
{
    private const string TopicPaymentCaptured = "payments.payment-captured";

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKERS"),
            GroupId = "payments",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe("orders.order-placed");

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string> result;
            try { result = consumer.Consume(stoppingToken); }
            catch (OperationCanceledException) { break; }

            while (true) // retry until the database accepts it
            {
                try { await Capture(result.Message.Value, stoppingToken); break; }
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

    private async Task Capture(string payload, CancellationToken ct)
    {
        Guid eventId, orderId;
        decimal amount;
        try
        {
            using var doc = JsonDocument.Parse(payload);
            eventId = Guid.Parse(doc.RootElement.GetProperty("eventId").GetString()!);
            orderId = Guid.Parse(doc.RootElement.GetProperty("orderId").GetString()!);
            amount = decimal.Parse(doc.RootElement.GetProperty("amount").GetString()!, CultureInfo.InvariantCulture);
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or FormatException or ArgumentNullException)
        {
            log.LogError("skipping malformed event");
            return;
        }

        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using (var dedupe = new NpgsqlCommand(
            "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING", conn, tx))
        {
            dedupe.Parameters.AddWithValue(eventId);
            if (await dedupe.ExecuteNonQueryAsync(ct) == 0) return; // duplicate delivery
        }

        var paymentId = Guid.NewGuid();
        await using (var capture = new NpgsqlCommand(
            """
            INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, 'CAPTURED')
            ON CONFLICT (order_id) DO NOTHING
            """, conn, tx))
        {
            capture.Parameters.AddWithValue(paymentId);
            capture.Parameters.AddWithValue(orderId);
            capture.Parameters.AddWithValue(amount);
            if (await capture.ExecuteNonQueryAsync(ct) == 0)
            {
                await tx.CommitAsync(ct); // this order was already paid; keep the dedupe record
                return;
            }
        }

        // A real service would call the payment provider here, using the
        // order ID as the provider's idempotency key.
        var evt = new Dictionary<string, string>
        {
            ["eventId"] = Guid.NewGuid().ToString(),
            ["eventType"] = "PaymentCaptured",
            ["orderId"] = orderId.ToString(),
            ["paymentId"] = paymentId.ToString(),
            ["amount"] = amount.ToString("F2", CultureInfo.InvariantCulture),
            ["occurredAt"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
        };
        await Outbox.Insert(conn, tx, "Payment", orderId.ToString(), "PaymentCaptured", TopicPaymentCaptured,
            JsonSerializer.Serialize(evt));
        await tx.CommitAsync(ct);
    }
}
