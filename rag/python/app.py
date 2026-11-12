"""RAG service (Python): ingests policy documents into Postgres + pgvector and answers
questions with hybrid retrieval, permission filtering, citations and refusal."""

import hashlib
import os
import re
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

DATABASE_URL = os.environ["DATABASE_URL"]
OLLAMA_URL = os.environ["OLLAMA_URL"]
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "qwen2.5:3b")
DOCS_DIR = Path(os.environ.get("DOCS_DIR", "/docs"))
MIN_SIMILARITY = float(os.environ.get("MIN_SIMILARITY", "0.55"))
MAX_CHUNK_CHARS = 1500
NOT_FOUND = "I couldn't find this in the policy documents."

SYSTEM_PROMPT = """You answer employee questions using only the numbered policy passages provided.
Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
Example of the format: Employees get 10 days of sick leave per year [3].
If the passages do not contain the answer, reply with exactly NOT_FOUND."""

RETRIEVAL_SQL = """
WITH semantic AS (
    SELECT id,
           1 - (embedding <=> %(vec)s::vector) AS similarity,
           row_number() OVER (ORDER BY embedding <=> %(vec)s::vector) AS rnk
    FROM chunks
    WHERE allowed_groups && %(groups)s
    ORDER BY embedding <=> %(vec)s::vector
    LIMIT 40
),
keyword AS (
    SELECT id,
           row_number() OVER (ORDER BY ts_rank_cd(content_tsv, q) DESC) AS rnk
    FROM chunks, websearch_to_tsquery('english', %(text)s) AS q
    WHERE content_tsv @@ q AND allowed_groups && %(groups)s
    ORDER BY ts_rank_cd(content_tsv, q) DESC
    LIMIT 40
)
SELECT c.document_id, c.title, c.section, c.content,
       COALESCE(s.similarity, 0) AS similarity,
       COALESCE(1.0 / (60 + s.rnk), 0) + COALESCE(1.0 / (60 + k.rnk), 0) AS score
FROM chunks c
LEFT JOIN semantic s ON s.id = c.id
LEFT JOIN keyword  k ON k.id = c.id
WHERE s.id IS NOT NULL OR k.id IS NOT NULL
ORDER BY score DESC
LIMIT 6
"""

pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=True)
ollama = httpx.Client(base_url=OLLAMA_URL, timeout=300)
app = FastAPI(title="RAG")


# ---------- Ingestion ----------

def parse_document(path: Path) -> tuple[str, list[str], str]:
    """Return (title, groups, body) from a markdown file with a small header."""
    text = path.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.S)
    if not match:
        raise ValueError(f"{path.name} has no header")
    header = dict(line.split(":", 1) for line in match.group(1).splitlines() if ":" in line)
    groups = [g.strip() for g in header["groups"].split(",") if g.strip()]
    return header["title"].strip(), groups, match.group(2)


def split_long(text: str) -> list[str]:
    """Split text longer than MAX_CHUNK_CHARS on paragraph boundaries."""
    parts, current = [], ""
    for para in text.split("\n\n"):
        if current and len(current) + len(para) + 2 > MAX_CHUNK_CHARS:
            parts.append(current)
            current = para
        else:
            current = f"{current}\n\n{para}" if current else para
    if current:
        parts.append(current)
    return parts


def chunk_document(title: str, body: str) -> list[tuple[str, str]]:
    """One chunk per ## section; each chunk is prefixed with title and section."""
    chunks = []
    for block in re.split(r"^## ", body, flags=re.M)[1:]:
        section, _, text = block.partition("\n")
        for part in split_long(text.strip()):
            chunks.append((section.strip(), f"{title} > {section.strip()}\n\n{part}"))
    return chunks


def embed(texts: list[str]) -> list[list[float]]:
    resp = ollama.post("/api/embed", json={"model": EMBED_MODEL, "input": texts})
    resp.raise_for_status()
    return resp.json()["embeddings"]


def to_vector(values: list[float]) -> str:
    return "[" + ",".join(repr(v) for v in values) + "]"


@app.post("/ingest")
def ingest():
    stats = {"documents": 0, "chunks": 0, "embedded": 0, "skipped": 0, "deleted": 0}
    seen_documents = []
    with pool.connection() as conn:
        for path in sorted(DOCS_DIR.glob("*.md")):
            document_id = path.stem
            title, groups, body = parse_document(path)
            chunks = chunk_document(title, body)
            stats["documents"] += 1
            stats["chunks"] += len(chunks)
            seen_documents.append(document_id)

            existing = dict(conn.execute(
                "SELECT chunk_index, content_hash FROM chunks WHERE document_id = %s", (document_id,)
            ).fetchall())
            hashes = [
                hashlib.sha256(f"{EMBED_MODEL}|{','.join(groups)}|{content}".encode()).hexdigest()
                for _, content in chunks
            ]
            changed = [i for i, h in enumerate(hashes) if existing.get(i) != h]
            stats["skipped"] += len(chunks) - len(changed)

            if changed:
                vectors = embed([f"search_document: {chunks[i][1]}" for i in changed])
                with conn.transaction():
                    for i, vec in zip(changed, vectors):
                        section, content = chunks[i]
                        conn.execute(
                            """INSERT INTO chunks (document_id, chunk_index, title, section, content,
                                   allowed_groups, content_hash, embedding_model, embedding)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
                               ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                                   title = EXCLUDED.title, section = EXCLUDED.section,
                                   content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
                                   content_hash = EXCLUDED.content_hash,
                                   embedding_model = EXCLUDED.embedding_model,
                                   embedding = EXCLUDED.embedding, updated_at = now()""",
                            (document_id, i, title, section, content, groups, hashes[i], EMBED_MODEL, to_vector(vec)),
                        )
                stats["embedded"] += len(changed)

            # Sections removed from this document.
            stats["deleted"] += conn.execute(
                "DELETE FROM chunks WHERE document_id = %s AND chunk_index >= %s", (document_id, len(chunks))
            ).rowcount

        # Documents removed from the folder.
        stats["deleted"] += conn.execute(
            "DELETE FROM chunks WHERE NOT (document_id = ANY(%s))", (seen_documents,)
        ).rowcount
    return stats


# ---------- Question answering ----------

class Question(BaseModel):
    question: str
    groups: list[str]


def not_found():
    return {"found": False, "answer": NOT_FOUND, "citations": []}


@app.post("/ask")
def ask(q: Question):
    if not q.question.strip() or not q.groups:
        return JSONResponse({"error": "question and groups are required"}, status_code=400)

    vec = to_vector(embed([f"search_query: {q.question}"])[0])
    with pool.connection() as conn:
        rows = conn.execute(RETRIEVAL_SQL, {"vec": vec, "text": q.question, "groups": q.groups}).fetchall()

    # Refuse without calling the model when nothing is relevant enough.
    if not rows or max(r[4] for r in rows) < MIN_SIMILARITY:
        return not_found()

    passages = "\n\n".join(f"[{i}] {r[3]}" for i, r in enumerate(rows, start=1))
    resp = ollama.post("/api/chat", json={
        "model": CHAT_MODEL,
        "stream": False,
        "options": {"temperature": 0},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Passages:\n{passages}\n\nQuestion: {q.question}"},
        ],
    })
    resp.raise_for_status()
    answer = resp.json()["message"]["content"].strip()
    if "NOT_FOUND" in answer:
        return not_found()

    # Citations: passages referenced as [n], de-duplicated, in order of first mention.
    cited, seen = [], set()
    for n in re.findall(r"\[(\d+)\]", answer):
        i = int(n) - 1
        if 0 <= i < len(rows) and (rows[i][0], rows[i][2]) not in seen:
            seen.add((rows[i][0], rows[i][2]))
            cited.append(rows[i])
    if not cited:
        cited = [rows[0]]
    return {
        "found": True,
        "answer": answer,
        "citations": [{"documentId": r[0], "title": r[1], "section": r[2]} for r in cited],
    }


@app.get("/health")
def health():
    return {"status": "ok"}
