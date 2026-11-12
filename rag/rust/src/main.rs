//! RAG service (Rust): ingests policy documents into Postgres + pgvector and answers
//! questions with hybrid retrieval, permission filtering, citations and refusal.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::Arc, time::Duration};

const MAX_CHUNK_CHARS: usize = 1500;
const NOT_FOUND: &str = "I couldn't find this in the policy documents.";
const SYSTEM_PROMPT: &str = "You answer employee questions using only the numbered policy passages provided.
Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
Example of the format: Employees get 10 days of sick leave per year [3].
If the passages do not contain the answer, reply with exactly NOT_FOUND.";

const RETRIEVAL_SQL: &str = "
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
LIMIT 6";

struct AppState {
    db: PgPool,
    http: reqwest::Client,
    ollama_url: String,
    embed_model: String,
    chat_model: String,
    docs_dir: PathBuf,
    min_similarity: f64,
}

type AppResult<T> = Result<T, AppError>;
struct AppError(StatusCode, String);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}
impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}
impl From<reqwest::Error> for AppError {
    fn from(e: reqwest::Error) -> Self {
        AppError(StatusCode::BAD_GATEWAY, format!("ollama: {e}"))
    }
}

// ---------- Ollama ----------

impl AppState {
    async fn embed(&self, texts: Vec<String>) -> AppResult<Vec<Vec<f64>>> {
        let resp: Value = self
            .http
            .post(format!("{}/api/embed", self.ollama_url))
            .json(&json!({ "model": self.embed_model, "input": texts }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        serde_json::from_value(resp["embeddings"].clone())
            .map_err(|e| AppError(StatusCode::BAD_GATEWAY, e.to_string()))
    }

    async fn chat(&self, user: String) -> AppResult<String> {
        let resp: Value = self
            .http
            .post(format!("{}/api/chat", self.ollama_url))
            .json(&json!({
                "model": self.chat_model,
                "stream": false,
                "options": { "temperature": 0 },
                "messages": [
                    { "role": "system", "content": SYSTEM_PROMPT },
                    { "role": "user", "content": user },
                ],
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp["message"]["content"].as_str().unwrap_or_default().trim().to_string())
    }
}

fn to_vector(v: &[f64]) -> String {
    let parts: Vec<String> = v.iter().map(|x| x.to_string()).collect();
    format!("[{}]", parts.join(","))
}

// ---------- Ingestion ----------

fn parse_document(text: &str) -> Option<(String, Vec<String>, String)> {
    let re = Regex::new(r"(?s)^---\n(.*?)\n---\n(.*)$").unwrap();
    let caps = re.captures(text)?;
    let mut header = HashMap::new();
    for line in caps[1].lines() {
        if let Some((k, v)) = line.split_once(':') {
            header.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    let groups = header.get("groups")?.split(',').map(str::trim).filter(|g| !g.is_empty()).map(String::from).collect();
    Some((header.get("title")?.clone(), groups, caps[2].to_string()))
}

/// Split text longer than MAX_CHUNK_CHARS on paragraph boundaries.
fn split_long(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for para in text.split("\n\n") {
        if !current.is_empty() && current.len() + para.len() + 2 > MAX_CHUNK_CHARS {
            parts.push(std::mem::replace(&mut current, para.to_string()));
        } else if current.is_empty() {
            current = para.to_string();
        } else {
            current = format!("{current}\n\n{para}");
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// One chunk per ## section; each chunk is prefixed with title and section.
fn chunk_document(title: &str, body: &str) -> Vec<(String, String)> {
    let re = Regex::new(r"(?m)^## ").unwrap();
    let mut chunks = Vec::new();
    for block in re.split(body).skip(1) {
        let (section, text) = block.split_once('\n').unwrap_or((block, ""));
        let section = section.trim();
        for part in split_long(text.trim()) {
            chunks.push((section.to_string(), format!("{title} > {section}\n\n{part}")));
        }
    }
    chunks
}

async fn ingest(State(s): State<Arc<AppState>>) -> AppResult<Json<Value>> {
    let (mut documents, mut total, mut embedded, mut skipped, mut deleted) = (0, 0, 0, 0, 0u64);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&s.docs_dir)
        .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "md"))
        .collect();
    paths.sort();
    let mut seen: Vec<String> = Vec::new();

    for path in paths {
        let document_id = path.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let (title, groups, body) = parse_document(&text)
            .ok_or_else(|| AppError(StatusCode::INTERNAL_SERVER_ERROR, format!("{document_id} has no header")))?;
        let chunks = chunk_document(&title, &body);
        documents += 1;
        total += chunks.len();
        seen.push(document_id.clone());

        let existing: HashMap<i32, String> =
            sqlx::query("SELECT chunk_index, content_hash FROM chunks WHERE document_id = $1")
                .bind(&document_id)
                .fetch_all(&s.db)
                .await?
                .iter()
                .map(|r| (r.get("chunk_index"), r.get("content_hash")))
                .collect();
        let hashes: Vec<String> = chunks
            .iter()
            .map(|(_, content)| {
                hex::encode(Sha256::digest(format!("{}|{}|{}", s.embed_model, groups.join(","), content)))
            })
            .collect();
        let changed: Vec<usize> =
            (0..chunks.len()).filter(|&i| existing.get(&(i as i32)) != Some(&hashes[i])).collect();
        skipped += chunks.len() - changed.len();

        if !changed.is_empty() {
            let vectors = s.embed(changed.iter().map(|&i| format!("search_document: {}", chunks[i].1)).collect()).await?;
            let mut tx = s.db.begin().await?;
            for (j, &i) in changed.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO chunks (document_id, chunk_index, title, section, content,
                         allowed_groups, content_hash, embedding_model, embedding)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::vector)
                     ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                         title = EXCLUDED.title, section = EXCLUDED.section,
                         content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
                         content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
                         embedding = EXCLUDED.embedding, updated_at = now()",
                )
                .bind(&document_id).bind(i as i32).bind(&title).bind(&chunks[i].0).bind(&chunks[i].1)
                .bind(&groups).bind(&hashes[i]).bind(&s.embed_model).bind(to_vector(&vectors[j]))
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            embedded += changed.len();
        }

        // Sections removed from this document.
        deleted += sqlx::query("DELETE FROM chunks WHERE document_id = $1 AND chunk_index >= $2")
            .bind(&document_id)
            .bind(chunks.len() as i32)
            .execute(&s.db)
            .await?
            .rows_affected();
    }

    // Documents removed from the folder.
    deleted += sqlx::query("DELETE FROM chunks WHERE NOT (document_id = ANY($1))")
        .bind(&seen)
        .execute(&s.db)
        .await?
        .rows_affected();

    Ok(Json(json!({
        "documents": documents, "chunks": total, "embedded": embedded, "skipped": skipped, "deleted": deleted
    })))
}

// ---------- Question answering ----------

#[derive(Deserialize)]
struct Question {
    question: Option<String>,
    groups: Option<Vec<String>>,
}

async fn ask(State(s): State<Arc<AppState>>, body: Option<Json<Question>>) -> AppResult<Json<Value>> {
    let (question, groups) = match body {
        Some(Json(Question { question: Some(q), groups: Some(g) })) if !q.trim().is_empty() && !g.is_empty() => (q, g),
        _ => return Err(AppError(StatusCode::BAD_REQUEST, "question and groups are required".into())),
    };
    let not_found = json!({ "found": false, "answer": NOT_FOUND, "citations": [] });

    let vec = s.embed(vec![format!("search_query: {question}")]).await?;
    let rows = sqlx::query(RETRIEVAL_SQL)
        .bind(to_vector(&vec[0]))
        .bind(&question)
        .bind(&groups)
        .fetch_all(&s.db)
        .await?;

    // Refuse without calling the model when nothing is relevant enough.
    let best = rows.iter().map(|r| r.get::<f64, _>("similarity")).fold(0.0, f64::max);
    if rows.is_empty() || best < s.min_similarity {
        return Ok(Json(not_found));
    }

    let passages: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| format!("[{}] {}", i + 1, r.get::<String, _>("content")))
        .collect();
    let answer = s.chat(format!("Passages:\n{}\n\nQuestion: {question}", passages.join("\n\n"))).await?;
    if answer.contains("NOT_FOUND") {
        return Ok(Json(not_found));
    }

    // Citations: passages referenced as [n], de-duplicated, in order of first mention.
    let cite = |r: &sqlx::postgres::PgRow| {
        json!({
            "documentId": r.get::<String, _>("document_id"),
            "title": r.get::<String, _>("title"),
            "section": r.get::<String, _>("section"),
        })
    };
    let mut citations = Vec::new();
    let mut seen = HashSet::new();
    for caps in Regex::new(r"\[(\d+)\]").unwrap().captures_iter(&answer) {
        let i: usize = caps[1].parse().unwrap_or(0);
        if i == 0 || i > rows.len() {
            continue;
        }
        let r = &rows[i - 1];
        if seen.insert((r.get::<String, _>("document_id"), r.get::<String, _>("section"))) {
            citations.push(cite(r));
        }
    }
    if citations.is_empty() {
        citations.push(cite(&rows[0]));
    }
    Ok(Json(json!({ "found": true, "answer": answer, "citations": citations })))
}

fn env(key: &str, fallback: &str) -> String {
    std::env::var(key).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| fallback.to_string())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .await
        .expect("database");
    let state = Arc::new(AppState {
        db,
        http: reqwest::Client::builder().timeout(Duration::from_secs(300)).build().unwrap(),
        ollama_url: std::env::var("OLLAMA_URL").expect("OLLAMA_URL"),
        embed_model: env("EMBED_MODEL", "nomic-embed-text"),
        chat_model: env("CHAT_MODEL", "qwen2.5:3b"),
        docs_dir: PathBuf::from(env("DOCS_DIR", "/docs")),
        min_similarity: env("MIN_SIMILARITY", "0.55").parse().unwrap_or(0.55),
    });

    let app = Router::new()
        .route("/ingest", post(ingest))
        .route("/ask", post(ask))
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .with_state(state);
    let port = env("PORT", "8080");
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.expect("bind");
    tracing::info!(port, "rag service listening");
    axum::serve(listener, app).await.expect("server");
}
