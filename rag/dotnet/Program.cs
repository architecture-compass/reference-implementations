// RAG service (C#): ingests policy documents into Postgres + pgvector and answers
// questions with hybrid retrieval, permission filtering, citations and refusal.

using Npgsql;
using Rag;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Env.Get("PORT", "8080")}");
builder.Services.AddSingleton(NpgsqlDataSource.Create(Env.Require("DATABASE_URL").ToNpgsqlConnectionString()));
builder.Services.AddHttpClient<Ollama>(c =>
{
    c.BaseAddress = new Uri(Env.Require("OLLAMA_URL"));
    c.Timeout = TimeSpan.FromMinutes(5);
});
builder.Services.AddSingleton<Ingestor>();
builder.Services.AddSingleton<Answerer>();

var app = builder.Build();

app.MapPost("/ingest", async (Ingestor ingestor) => Results.Ok(await ingestor.IngestAsync()));

app.MapPost("/ask", async (Question? q, Answerer answerer) =>
    q?.question is { Length: > 0 } question && !string.IsNullOrWhiteSpace(question) && q.groups is { Length: > 0 } groups
        ? Results.Ok(await answerer.AskAsync(question, groups))
        : Results.BadRequest(new { error = "question and groups are required" }));

app.MapGet("/health", () => new { status = "ok" });

app.Run();

record Question(string? question, string[]? groups);

namespace Rag
{
    static class Env
    {
        public static string Get(string key, string fallback) =>
            Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

        public static string Require(string key) =>
            Environment.GetEnvironmentVariable(key) ?? throw new InvalidOperationException($"{key} is required");

        /// <summary>Convert postgres://user:pass@host:port/db to an Npgsql connection string.</summary>
        public static string ToNpgsqlConnectionString(this string url)
        {
            var uri = new Uri(url);
            var userInfo = uri.UserInfo.Split(':', 2);
            return new NpgsqlConnectionStringBuilder
            {
                Host = uri.Host,
                Port = uri.Port > 0 ? uri.Port : 5432,
                Database = uri.AbsolutePath.TrimStart('/'),
                Username = Uri.UnescapeDataString(userInfo[0]),
                Password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : null,
            }.ConnectionString;
        }
    }
}
