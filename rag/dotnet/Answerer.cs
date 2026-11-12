using System.Text.RegularExpressions;
using Npgsql;

namespace Rag;

record Citation(string documentId, string title, string section);
record Answer(bool found, string answer, List<Citation> citations);

/// <summary>Hybrid retrieval, permission filtering, refusal, generation and citations.</summary>
partial class Answerer(NpgsqlDataSource db, Ollama ollama)
{
    record Passage(string DocumentId, string Title, string Section, string Content, double Similarity);

    const string NotFound = "I couldn't find this in the policy documents.";

    const string SystemPrompt = """
        You answer employee questions using only the numbered policy passages provided.
        Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
        Example of the format: Employees get 10 days of sick leave per year [3].
        If the passages do not contain the answer, reply with exactly NOT_FOUND.
        """;

    const string RetrievalSql = """
        WITH semantic AS (
            SELECT id,
                   1 - (embedding <=> $1::vector) AS similarity,
                   row_number() OVER (ORDER BY embedding <=> $1::vector) AS rnk
            FROM chunks
            WHERE allowed_groups && $3
            ORDER BY embedding <=> $1::vector
            LIMIT 40
        ),
        keyword AS (
            SELECT id,
                   row_number() OVER (ORDER BY ts_rank_cd(content_tsv, q) DESC) AS rnk
            FROM chunks, websearch_to_tsquery('english', $2) AS q
            WHERE content_tsv @@ q AND allowed_groups && $3
            ORDER BY ts_rank_cd(content_tsv, q) DESC
            LIMIT 40
        )
        SELECT c.document_id, c.title, c.section, c.content,
               COALESCE(s.similarity, 0)::float8 AS similarity,
               (COALESCE(1.0 / (60 + s.rnk), 0) + COALESCE(1.0 / (60 + k.rnk), 0))::float8 AS score
        FROM chunks c
        LEFT JOIN semantic s ON s.id = c.id
        LEFT JOIN keyword  k ON k.id = c.id
        WHERE s.id IS NOT NULL OR k.id IS NOT NULL
        ORDER BY score DESC
        LIMIT 6
        """;

    readonly double minSimilarity = double.Parse(Env.Get("MIN_SIMILARITY", "0.55"),
        System.Globalization.CultureInfo.InvariantCulture);

    [GeneratedRegex(@"\[(\d+)\]")]
    private static partial Regex CitationRef();

    public async Task<Answer> AskAsync(string question, string[] groups)
    {
        var notFound = new Answer(false, NotFound, []);
        var vec = Ollama.ToVector((await ollama.EmbedAsync(["search_query: " + question]))[0]);

        var rows = new List<Passage>();
        await using (var cmd = db.CreateCommand(RetrievalSql))
        {
            cmd.Parameters.AddWithValue(vec);
            cmd.Parameters.AddWithValue(question);
            cmd.Parameters.AddWithValue(groups);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                rows.Add(new Passage(reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetString(3), reader.GetDouble(4)));
        }

        // Refuse without calling the model when nothing is relevant enough.
        if (rows.Count == 0 || rows.Max(r => r.Similarity) < minSimilarity) return notFound;

        var passages = string.Join("\n\n", rows.Select((r, i) => $"[{i + 1}] {r.Content}"));
        var answer = await ollama.ChatAsync(SystemPrompt, $"Passages:\n{passages}\n\nQuestion: {question}");
        if (answer.Contains("NOT_FOUND")) return notFound;

        // Citations: passages referenced as [n], de-duplicated, in order of first mention.
        var citations = new List<Citation>();
        var seen = new HashSet<string>();
        foreach (Match m in CitationRef().Matches(answer))
        {
            var i = int.Parse(m.Groups[1].Value) - 1;
            if (i < 0 || i >= rows.Count) continue;
            var p = rows[i];
            if (seen.Add(p.DocumentId + "|" + p.Section))
                citations.Add(new Citation(p.DocumentId, p.Title, p.Section));
        }
        if (citations.Count == 0)
            citations.Add(new Citation(rows[0].DocumentId, rows[0].Title, rows[0].Section));
        return new Answer(true, answer, citations);
    }
}
