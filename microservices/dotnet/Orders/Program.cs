// Orders service (C#): accepts orders, writes them with an OrderPlaced event to the
// transactional outbox, relays the outbox to Kafka, and confirms orders when
// PaymentCaptured arrives. Catalog calls use timeout, retries and a circuit breaker
// with a last-known-price fallback.
using System.Globalization;
using System.Text.Json;
using Npgsql;
using Orders;

const string TopicOrderPlaced = "orders.order-placed";

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("PORT") ?? "8080"}");

var db = new NpgsqlDataSourceBuilder(Db.ConnectionString(Environment.GetEnvironmentVariable("DATABASE_URL")!)).Build();
builder.Services.AddSingleton(db);
builder.Services.AddSingleton(new Catalog(Environment.GetEnvironmentVariable("CATALOG_URL")!));
builder.Services.AddHostedService<OutboxRelay>();
builder.Services.AddHostedService<PaymentCapturedConsumer>();

var app = builder.Build();

app.MapPost("/orders", async (HttpRequest http, NpgsqlDataSource db, Catalog catalog) =>
{
    var key = http.Headers["Idempotency-Key"].ToString();
    if (string.IsNullOrEmpty(key))
        return Results.Json(new { error = "Idempotency-Key header is required" }, statusCode: 400);

    OrderRequest? req;
    try { req = await http.ReadFromJsonAsync<OrderRequest>(); }
    catch (Exception) { req = null; }
    if (req is null || string.IsNullOrEmpty(req.CustomerId) || string.IsNullOrEmpty(req.Sku) || req.Quantity is null or < 1)
        return Results.Json(new { error = "customerId, sku and a positive quantity are required" }, statusCode: 400);

    if (await FindByKey(db, key) is { } existing) // same key seen before
        return Results.Json(existing, statusCode: 202);

    CatalogPrice price;
    try { price = await catalog.GetPrice(req.Sku); }
    catch (UnknownSkuException) { return Results.Json(new { error = "unknown sku" }, statusCode: 404); }
    catch (CatalogUnavailableException e) { return Results.Json(new { error = e.Message }, statusCode: 503); }

    var amount = Math.Round(decimal.Parse(price.Price, CultureInfo.InvariantCulture) * req.Quantity.Value, 2);
    var amountText = amount.ToString("F2", CultureInfo.InvariantCulture);
    var orderId = Guid.NewGuid();
    var evt = new Dictionary<string, string>
    {
        ["eventId"] = Guid.NewGuid().ToString(),
        ["eventType"] = "OrderPlaced",
        ["orderId"] = orderId.ToString(),
        ["customerId"] = req.CustomerId,
        ["amount"] = amountText,
        ["currency"] = price.Currency,
        ["occurredAt"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
    };

    // The order and its event commit together, or not at all.
    await using var conn = await db.OpenConnectionAsync();
    await using var tx = await conn.BeginTransactionAsync();
    try
    {
        await using (var cmd = new NpgsqlCommand(
            """
            INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)
            """, conn, tx))
        {
            cmd.Parameters.AddWithValue(orderId);
            cmd.Parameters.AddWithValue(req.CustomerId);
            cmd.Parameters.AddWithValue(req.Sku);
            cmd.Parameters.AddWithValue(req.Quantity.Value);
            cmd.Parameters.AddWithValue(amount);
            cmd.Parameters.AddWithValue(price.Currency);
            cmd.Parameters.AddWithValue(key);
            await cmd.ExecuteNonQueryAsync();
        }
        await Outbox.Insert(conn, tx, "Order", orderId.ToString(), "OrderPlaced", TopicOrderPlaced, JsonSerializer.Serialize(evt));
        await tx.CommitAsync();
    }
    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
    {
        await tx.RollbackAsync(); // concurrent request with the same key
        return Results.Json(await FindByKey(db, key), statusCode: 202);
    }

    return Results.Json(new OrderView(orderId.ToString(), "PENDING", amountText, price.Currency), statusCode: 202);
});

app.MapGet("/orders/{id}", async (string id, NpgsqlDataSource db) =>
{
    var order = await Query(db, "SELECT id::text, status, amount, currency FROM orders WHERE id::text = $1", id);
    return order is null ? Results.Json(new { error = "order not found" }, statusCode: 404) : Results.Json(order);
});

app.MapGet("/health", () => Results.Json(new { status = "ok" }));

app.Run();

static Task<OrderView?> FindByKey(NpgsqlDataSource db, string key) =>
    Query(db, "SELECT id::text, status, amount, currency FROM orders WHERE idempotency_key = $1", key);

static async Task<OrderView?> Query(NpgsqlDataSource db, string sql, string param)
{
    await using var cmd = db.CreateCommand(sql);
    cmd.Parameters.AddWithValue(param);
    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync()) return null;
    return new OrderView(reader.GetString(0), reader.GetString(1),
        reader.GetDecimal(2).ToString("F2", CultureInfo.InvariantCulture), reader.GetString(3));
}

record OrderRequest(string? CustomerId, string? Sku, int? Quantity);
record OrderView(string OrderId, string Status, string Amount, string Currency);
