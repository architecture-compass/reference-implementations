// Payments service (C#): consumes OrderPlaced, captures each order's payment at most
// once, and publishes PaymentCaptured through its own transactional outbox.
using Npgsql;
using Payments;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("PORT") ?? "8080"}");

builder.Services.AddSingleton(
    new NpgsqlDataSourceBuilder(Db.ConnectionString(Environment.GetEnvironmentVariable("DATABASE_URL")!)).Build());
builder.Services.AddHostedService<OutboxRelay>();
builder.Services.AddHostedService<OrderPlacedConsumer>();

var app = builder.Build();
app.MapGet("/health", () => Results.Json(new { status = "ok" }));
app.Run();
