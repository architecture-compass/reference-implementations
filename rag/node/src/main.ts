// RAG service (TypeScript): ingests policy documents into Postgres + pgvector and answers
// questions with hybrid retrieval, permission filtering, citations and refusal.
import Fastify from "fastify";
import pg from "pg";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { basename, join } from "node:path";

const OLLAMA_URL = process.env.OLLAMA_URL!;
const EMBED_MODEL = process.env.EMBED_MODEL ?? "nomic-embed-text";
const CHAT_MODEL = process.env.CHAT_MODEL ?? "qwen2.5:3b";
const DOCS_DIR = process.env.DOCS_DIR ?? "/docs";
const MIN_SIMILARITY = Number(process.env.MIN_SIMILARITY ?? "0.55");
const MAX_CHUNK_CHARS = 1500;
const NOT_FOUND = "I couldn't find this in the policy documents.";

const SYSTEM_PROMPT = `You answer employee questions using only the numbered policy passages provided.
Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
Example of the format: Employees get 10 days of sick leave per year [3].
If the passages do not contain the answer, reply with exactly NOT_FOUND.`;

const RETRIEVAL_SQL = `
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
LIMIT 6`;

const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 5 });

// ---------- Ollama ----------

async function ollama<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${OLLAMA_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(300_000),
  });
  if (!res.ok) throw new Error(`ollama ${path} returned ${res.status}`);
  return (await res.json()) as T;
}

async function embed(texts: string[]): Promise<number[][]> {
  return (await ollama<{ embeddings: number[][] }>("/api/embed", { model: EMBED_MODEL, input: texts })).embeddings;
}

const toVector = (v: number[]) => `[${v.join(",")}]`;

// ---------- Ingestion ----------

function parseDocument(text: string) {
  const m = text.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!m) throw new Error("document has no header");
  const header: Record<string, string> = {};
  for (const line of m[1].split("\n")) {
    const i = line.indexOf(":");
    if (i > 0) header[line.slice(0, i).trim()] = line.slice(i + 1).trim();
  }
  const groups = header.groups.split(",").map((g) => g.trim()).filter(Boolean);
  return { title: header.title, groups, body: m[2] };
}

/** Split text longer than MAX_CHUNK_CHARS on paragraph boundaries. */
function splitLong(text: string): string[] {
  const parts: string[] = [];
  let current = "";
  for (const para of text.split("\n\n")) {
    if (current && current.length + para.length + 2 > MAX_CHUNK_CHARS) {
      parts.push(current);
      current = para;
    } else {
      current = current ? `${current}\n\n${para}` : para;
    }
  }
  if (current) parts.push(current);
  return parts;
}

/** One chunk per ## section; each chunk is prefixed with title and section. */
function chunkDocument(title: string, body: string) {
  const chunks: { section: string; content: string }[] = [];
  for (const block of body.split(/^## /m).slice(1)) {
    const nl = block.indexOf("\n");
    const section = (nl < 0 ? block : block.slice(0, nl)).trim();
    const text = nl < 0 ? "" : block.slice(nl + 1).trim();
    for (const part of splitLong(text)) chunks.push({ section, content: `${title} > ${section}\n\n${part}` });
  }
  return chunks;
}

async function ingest() {
  const stats = { documents: 0, chunks: 0, embedded: 0, skipped: 0, deleted: 0 };
  const files = (await readdir(DOCS_DIR)).filter((f) => f.endsWith(".md")).sort();
  const seen: string[] = [];

  for (const file of files) {
    const documentId = basename(file, ".md");
    const { title, groups, body } = parseDocument(await readFile(join(DOCS_DIR, file), "utf-8"));
    const chunks = chunkDocument(title, body);
    stats.documents++;
    stats.chunks += chunks.length;
    seen.push(documentId);

    const { rows } = await db.query<{ chunk_index: number; content_hash: string }>(
      "SELECT chunk_index, content_hash FROM chunks WHERE document_id = $1", [documentId]);
    const existing = new Map(rows.map((r) => [r.chunk_index, r.content_hash]));
    const hashes = chunks.map((c) =>
      createHash("sha256").update(`${EMBED_MODEL}|${groups.join(",")}|${c.content}`).digest("hex"));
    const changed = hashes.flatMap((h, i) => (existing.get(i) === h ? [] : [i]));
    stats.skipped += chunks.length - changed.length;

    if (changed.length > 0) {
      const vectors = await embed(changed.map((i) => `search_document: ${chunks[i].content}`));
      const client = await db.connect();
      try {
        await client.query("BEGIN");
        for (const [j, i] of changed.entries()) {
          await client.query(
            `INSERT INTO chunks (document_id, chunk_index, title, section, content,
                 allowed_groups, content_hash, embedding_model, embedding)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::vector)
             ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                 title = EXCLUDED.title, section = EXCLUDED.section,
                 content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
                 content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
                 embedding = EXCLUDED.embedding, updated_at = now()`,
            [documentId, i, title, chunks[i].section, chunks[i].content, groups, hashes[i], EMBED_MODEL,
              toVector(vectors[j])]);
        }
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
      stats.embedded += changed.length;
    }

    // Sections removed from this document.
    stats.deleted += (await db.query("DELETE FROM chunks WHERE document_id = $1 AND chunk_index >= $2",
      [documentId, chunks.length])).rowCount ?? 0;
  }

  // Documents removed from the folder.
  stats.deleted += (await db.query("DELETE FROM chunks WHERE NOT (document_id = ANY($1))", [seen])).rowCount ?? 0;
  return stats;
}

// ---------- Question answering ----------

type Passage = { document_id: string; title: string; section: string; content: string; similarity: number };

async function ask(question: string, groups: string[]) {
  const notFound = { found: false, answer: NOT_FOUND, citations: [] as unknown[] };
  const [vec] = await embed([`search_query: ${question}`]);
  const { rows } = await db.query<Passage>(RETRIEVAL_SQL, [toVector(vec), question, groups]);

  // Refuse without calling the model when nothing is relevant enough.
  if (rows.length === 0 || Math.max(...rows.map((r) => r.similarity)) < MIN_SIMILARITY) return notFound;

  const passages = rows.map((r, i) => `[${i + 1}] ${r.content}`).join("\n\n");
  const chat = await ollama<{ message: { content: string } }>("/api/chat", {
    model: CHAT_MODEL,
    stream: false,
    options: { temperature: 0 },
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "user", content: `Passages:\n${passages}\n\nQuestion: ${question}` },
    ],
  });
  const answer = chat.message.content.trim();
  if (answer.includes("NOT_FOUND")) return notFound;

  // Citations: passages referenced as [n], de-duplicated, in order of first mention.
  const cited: Passage[] = [];
  const seen = new Set<string>();
  for (const m of answer.matchAll(/\[(\d+)\]/g)) {
    const p = rows[Number(m[1]) - 1];
    if (p && !seen.has(`${p.document_id}|${p.section}`)) {
      seen.add(`${p.document_id}|${p.section}`);
      cited.push(p);
    }
  }
  if (cited.length === 0) cited.push(rows[0]);
  return {
    found: true,
    answer,
    citations: cited.map((p) => ({ documentId: p.document_id, title: p.title, section: p.section })),
  };
}

const app = Fastify({ logger: { level: "warn" } });
app.post("/ingest", async () => ingest());
app.post<{ Body: { question?: string; groups?: string[] } }>("/ask", async (req, reply) => {
  const { question, groups } = req.body ?? {};
  if (!question?.trim() || !Array.isArray(groups) || groups.length === 0) {
    return reply.code(400).send({ error: "question and groups are required" });
  }
  return ask(question, groups);
});
app.get("/health", async () => ({ status: "ok" }));

app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) }).then(() => console.log("rag service listening"));
