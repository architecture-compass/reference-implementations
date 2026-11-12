using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

namespace Rag;

/// <summary>Embeddings and chat completions from a local Ollama server.</summary>
class Ollama(HttpClient http)
{
    public static readonly string EmbedModel = Env.Get("EMBED_MODEL", "nomic-embed-text");
    public static readonly string ChatModel = Env.Get("CHAT_MODEL", "qwen2.5:3b");

    public async Task<List<double[]>> EmbedAsync(IEnumerable<string> texts)
    {
        var resp = await http.PostAsJsonAsync("/api/embed", new { model = EmbedModel, input = texts.ToArray() });
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return body.GetProperty("embeddings").EnumerateArray()
            .Select(e => e.EnumerateArray().Select(x => x.GetDouble()).ToArray())
            .ToList();
    }

    public async Task<string> ChatAsync(string system, string user)
    {
        var resp = await http.PostAsJsonAsync("/api/chat", new
        {
            model = ChatModel,
            stream = false,
            options = new { temperature = 0 },
            messages = new[]
            {
                new { role = "system", content = system },
                new { role = "user", content = user },
            },
        });
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return body.GetProperty("message").GetProperty("content").GetString()?.Trim() ?? "";
    }

    /// <summary>pgvector text format: [0.1,0.2,...]</summary>
    public static string ToVector(double[] v) =>
        "[" + string.Join(",", v.Select(x => x.ToString("R", CultureInfo.InvariantCulture))) + "]";
}
