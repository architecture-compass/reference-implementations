using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;

namespace Rag;

/// <summary>Reads markdown documents, chunks them by section, and embeds new or changed chunks.</summary>
partial class Ingestor(NpgsqlDataSource db, Ollama ollama)
{
    const int MaxChunkChars = 1500;
    readonly string docsDir = Env.Get("DOCS_DIR", "/docs");

    record Chunk(string Section, string Content);

    [GeneratedRegex(@"^---\n(.*?)\n---\n(.*)$", RegexOptions.Singleline)]
    private static partial Regex Header();

    [GeneratedRegex(@"^## ", RegexOptions.Multiline)]
    private static partial Regex Section();

    static (string Title, string[] Groups, string Body) Parse(string text)
    {
        var m = Header().Match(text);
        if (!m.Success) throw new InvalidOperationException("document has no header");
        var header = m.Groups[1].Value.Split('\n')
            .Where(l => l.Contains(':'))
            .ToDictionary(l => l[..l.IndexOf(':')].Trim(), l => l[(l.IndexOf(':') + 1)..].Trim());
        var groups = header["groups"].Split(',').Select(g => g.Trim()).Where(g => g.Length > 0).ToArray();
        return (header["title"], groups, m.Groups[2].Value);
    }

    /// <summary>Split text longer than MaxChunkChars on paragraph boundaries.</summary>
    static List<string> SplitLong(string text)
    {
        var parts = new List<string>();
        var current = "";
        foreach (var para in text.Split("\n\n"))
        {
            if (current.Length > 0 && current.Length + para.Length + 2 > MaxChunkChars)
            {
                parts.Add(current);
                current = para;
            }
            else
            {
                current = current.Length == 0 ? para : current + "\n\n" + para;
            }
        }
        if (current.Length > 0) parts.Add(current);
        return parts;
    }

    /// <summary>One chunk per ## section; each chunk is prefixed with title and section.</summary>
    static List<Chunk> ChunkDocument(string title, string body)
    {
        var chunks = new List<Chunk>();
        foreach (var block in Section().Split(body).Skip(1))
        {
            var nl = block.IndexOf('\n');
            var section = (nl < 0 ? block : block[..nl]).Trim();
            var text = nl < 0 ? "" : block[(nl + 1)..].Trim();
            foreach (var part in SplitLong(text))
                chunks.Add(new Chunk(section, $"{title} > {section}\n\n{part}"));
        }
        return chunks;
    }

    static string Sha256(string s) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(s)));

    public async Task<Dictionary<string, int>> IngestAsync()
    {
        var stats = new Dictionary<string, int>
        {
            ["documents"] = 0, ["chunks"] = 0, ["embedded"] = 0, ["skipped"] = 0, ["deleted"] = 0,
        };
        var seen = new List<string>();
        await using var conn = await db.OpenConnectionAsync();

        foreach (var file in Directory.GetFiles(docsDir, "*.md").Order(StringComparer.Ordinal))
        {
            var documentId = Path.GetFileNameWithoutExtension(file);
            var (title, groups, body) = Parse(await File.ReadAllTextAsync(file));
            var chunks = ChunkDocument(title, body);
            stats["documents"]++;
            stats["chunks"] += chunks.Count;
            seen.Add(documentId);

            var existing = new Dictionary<int, string>();
            await using (var cmd = new NpgsqlCommand(
                "SELECT chunk_index, content_hash FROM chunks WHERE document_id = $1", conn))
            {
                cmd.Parameters.AddWithValue(documentId);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync()) existing[reader.GetInt32(0)] = reader.GetString(1);
            }

            var hashes = chunks.Select(c => Sha256($"{Ollama.EmbedModel}|{string.Join(",", groups)}|{c.Content}")).ToList();
            var changed = Enumerable.Range(0, chunks.Count)
                .Where(i => !existing.TryGetValue(i, out var h) || h != hashes[i]).ToList();
            stats["skipped"] += chunks.Count - changed.Count;

            if (changed.Count > 0)
            {
                var vectors = await ollama.EmbedAsync(changed.Select(i => "search_document: " + chunks[i].Content));
                await using var tx = await conn.BeginTransactionAsync();
                for (var j = 0; j < changed.Count; j++)
                {
                    var i = changed[j];
                    await using var cmd = new NpgsqlCommand("""
                        INSERT INTO chunks (document_id, chunk_index, title, section, content,
                            allowed_groups, content_hash, embedding_model, embedding)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::vector)
                        ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                            title = EXCLUDED.title, section = EXCLUDED.section,
                            content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
                            content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
                            embedding = EXCLUDED.embedding, updated_at = now()
                        """, conn, tx);
                    foreach (var p in new object[]
                             {
                                 documentId, i, title, chunks[i].Section, chunks[i].Content,
                                 groups, hashes[i], Ollama.EmbedModel, Ollama.ToVector(vectors[j]),
                             })
                        cmd.Parameters.AddWithValue(p);
                    await cmd.ExecuteNonQueryAsync();
                }
                await tx.CommitAsync();
                stats["embedded"] += changed.Count;
            }

            // Sections removed from this document.
            await using (var cmd = new NpgsqlCommand(
                "DELETE FROM chunks WHERE document_id = $1 AND chunk_index >= $2", conn))
            {
                cmd.Parameters.AddWithValue(documentId);
                cmd.Parameters.AddWithValue(chunks.Count);
                stats["deleted"] += await cmd.ExecuteNonQueryAsync();
            }
        }

        // Documents removed from the folder.
        await using (var cmd = new NpgsqlCommand("DELETE FROM chunks WHERE NOT (document_id = ANY($1))", conn))
        {
            cmd.Parameters.AddWithValue(seen.ToArray());
            stats["deleted"] += await cmd.ExecuteNonQueryAsync();
        }
        return stats;
    }
}
