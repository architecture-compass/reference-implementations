# RAG contract

Every language implementation exposes this API and follows these rules, so results are comparable across languages.

## API (port 8080 in the container, 8090 on your machine)

### `POST /ingest`

Reads every `*.md` file in `DOCS_DIR`, chunks it, embeds new or changed chunks, and removes chunks for deleted documents. Safe to run repeatedly.

```json
{ "documents": 5, "chunks": 20, "embedded": 20, "skipped": 0, "deleted": 0 }
```

### `POST /ask`

```json
{ "question": "How much parental leave do I get?", "groups": ["all-staff"] }
```

`groups` stands in for the identity your single sign-on would provide. A user only ever retrieves chunks whose `allowed_groups` overlap their groups.

Response:

```json
{
  "found": true,
  "answer": "Eligible employees receive 16 weeks of fully paid parental leave [1].",
  "citations": [
    { "documentId": "parental-leave", "title": "Parental Leave Policy", "section": "How much leave you get" }
  ]
}
```

When the documents don't contain the answer: `found: false`, a fixed answer `"I couldn't find this in the policy documents."` and no citations.

### `GET /health`

`200 {"status":"ok"}`

## Documents

Markdown files with a small header:

```markdown
---
title: Parental Leave Policy
groups: all-staff
---

## Section heading
Section text…
```

## Rules every implementation follows

**Chunking.** One chunk per `##` section. Chunk content is `"{title} > {section}\n\n{text}"`, so each chunk makes sense on its own. Sections longer than 1,500 characters are split on paragraph boundaries.

**Change detection.** `content_hash = sha256(embedding_model + "|" + groups joined by "," + "|" + content)`. Unchanged chunks are skipped, so re-running ingest costs nothing.

**Embeddings.** Ollama `POST /api/embed` with model `nomic-embed-text`. That model expects a task prefix: documents are embedded as `"search_document: " + content`, questions as `"search_query: " + question`.

**Retrieval.** Hybrid search in one SQL statement: the top 40 chunks by vector similarity and the top 40 by keyword rank, merged with reciprocal rank fusion (`1 / (60 + rank)` from each list), filtered by group, keeping the top 6. See [`retrieval.sql`](retrieval.sql).

**Refusal.** If no chunk has a cosine similarity of at least `MIN_SIMILARITY` (default `0.55`), answer "not found" without calling the language model.

**Generation.** Ollama `POST /api/chat`, model `qwen2.5:3b`, temperature 0, with this system prompt:

```text
You answer employee questions using only the numbered policy passages provided.
Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
Example of the format: Employees get 10 days of sick leave per year [3].
If the passages do not contain the answer, reply with exactly NOT_FOUND.
```

and this user message:

```text
Passages:
[1] {content of chunk 1}

[2] {content of chunk 2}
…

Question: {question}
```

If the reply contains `NOT_FOUND`, the response is "not found".

**Citations.** The passages referenced by `[n]` markers in the answer, de-duplicated by document and section, in order of first mention. If the model cites nothing, cite the top-ranked passage.

## Environment

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://app:app@postgres:5432/rag` |
| `OLLAMA_URL` | `http://ollama:11434` |
| `EMBED_MODEL` | `nomic-embed-text` |
| `CHAT_MODEL` | `qwen2.5:3b` |
| `DOCS_DIR` | `/docs` |
| `MIN_SIMILARITY` | `0.55` |
| `PORT` | `8080` |
