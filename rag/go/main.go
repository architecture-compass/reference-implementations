// RAG service (Go): ingests policy documents into Postgres + pgvector and answers
// questions with hybrid retrieval, permission filtering, citations and refusal.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxChunkChars = 1500
	notFound      = "I couldn't find this in the policy documents."
	systemPrompt  = `You answer employee questions using only the numbered policy passages provided.
Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
Example of the format: Employees get 10 days of sick leave per year [3].
If the passages do not contain the answer, reply with exactly NOT_FOUND.`
)

const retrievalSQL = `
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
LIMIT 6`

var (
	headerRe   = regexp.MustCompile(`(?s)^---\n(.*?)\n---\n(.*)$`)
	sectionRe  = regexp.MustCompile(`(?m)^## `)
	citationRe = regexp.MustCompile(`\[(\d+)\]`)
)

type config struct {
	ollamaURL, embedModel, chatModel, docsDir string
	minSimilarity                             float64
}

type server struct {
	db   *pgxpool.Pool
	cfg  config
	http *http.Client
}

// ---------- Ollama ----------

func (s *server) postJSON(ctx context.Context, path string, body, out any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.ollamaURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama %s returned %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (s *server) embed(ctx context.Context, texts []string) ([][]float64, error) {
	var out struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	err := s.postJSON(ctx, "/api/embed", map[string]any{"model": s.cfg.embedModel, "input": texts}, &out)
	return out.Embeddings, err
}

func toVector(v []float64) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = strconv.FormatFloat(x, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// ---------- Ingestion ----------

type chunk struct{ section, content string }

func parseDocument(text string) (title string, groups []string, body string, err error) {
	m := headerRe.FindStringSubmatch(text)
	if m == nil {
		return "", nil, "", errors.New("document has no header")
	}
	for _, line := range strings.Split(m[1], "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "title":
			title = strings.TrimSpace(value)
		case "groups":
			for _, g := range strings.Split(value, ",") {
				if g = strings.TrimSpace(g); g != "" {
					groups = append(groups, g)
				}
			}
		}
	}
	return title, groups, m[2], nil
}

// splitLong splits text longer than maxChunkChars on paragraph boundaries.
func splitLong(text string) []string {
	var parts []string
	current := ""
	for _, para := range strings.Split(text, "\n\n") {
		if current != "" && len(current)+len(para)+2 > maxChunkChars {
			parts = append(parts, current)
			current = para
		} else if current != "" {
			current += "\n\n" + para
		} else {
			current = para
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// chunkDocument makes one chunk per ## section, prefixed with title and section.
func chunkDocument(title, body string) []chunk {
	var chunks []chunk
	blocks := sectionRe.Split(body, -1)
	for _, block := range blocks[1:] {
		section, text, _ := strings.Cut(block, "\n")
		section = strings.TrimSpace(section)
		for _, part := range splitLong(strings.TrimSpace(text)) {
			chunks = append(chunks, chunk{section, title + " > " + section + "\n\n" + part})
		}
	}
	return chunks
}

func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats := map[string]int{"documents": 0, "chunks": 0, "embedded": 0, "skipped": 0, "deleted": 0}
	paths, _ := filepath.Glob(filepath.Join(s.cfg.docsDir, "*.md"))
	sort.Strings(paths)
	var seen []string

	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		documentID := strings.TrimSuffix(filepath.Base(path), ".md")
		title, groups, body, err := parseDocument(string(raw))
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": documentID + ": " + err.Error()})
			return
		}
		chunks := chunkDocument(title, body)
		stats["documents"]++
		stats["chunks"] += len(chunks)
		seen = append(seen, documentID)

		existing := map[int]string{}
		rows, err := s.db.Query(ctx, `SELECT chunk_index, content_hash FROM chunks WHERE document_id = $1`, documentID)
		if err == nil {
			for rows.Next() {
				var i int
				var h string
				if rows.Scan(&i, &h) == nil {
					existing[i] = h
				}
			}
			rows.Close()
		}

		hashes := make([]string, len(chunks))
		var changed []int
		for i, c := range chunks {
			sum := sha256.Sum256([]byte(s.cfg.embedModel + "|" + strings.Join(groups, ",") + "|" + c.content))
			hashes[i] = hex.EncodeToString(sum[:])
			if existing[i] != hashes[i] {
				changed = append(changed, i)
			}
		}
		stats["skipped"] += len(chunks) - len(changed)

		if len(changed) > 0 {
			inputs := make([]string, len(changed))
			for j, i := range changed {
				inputs[j] = "search_document: " + chunks[i].content
			}
			vectors, err := s.embed(ctx, inputs)
			if err != nil {
				writeJSON(w, 502, map[string]string{"error": err.Error()})
				return
			}
			err = pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
				for j, i := range changed {
					_, err := tx.Exec(ctx, `
						INSERT INTO chunks (document_id, chunk_index, title, section, content,
						    allowed_groups, content_hash, embedding_model, embedding)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::vector)
						ON CONFLICT (document_id, chunk_index) DO UPDATE SET
						    title = EXCLUDED.title, section = EXCLUDED.section,
						    content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
						    content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
						    embedding = EXCLUDED.embedding, updated_at = now()`,
						documentID, i, title, chunks[i].section, chunks[i].content,
						groups, hashes[i], s.cfg.embedModel, toVector(vectors[j]))
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				writeJSON(w, 500, map[string]string{"error": err.Error()})
				return
			}
			stats["embedded"] += len(changed)
		}

		// Sections removed from this document.
		tag, _ := s.db.Exec(ctx, `DELETE FROM chunks WHERE document_id = $1 AND chunk_index >= $2`, documentID, len(chunks))
		stats["deleted"] += int(tag.RowsAffected())
	}

	// Documents removed from the folder.
	tag, _ := s.db.Exec(ctx, `DELETE FROM chunks WHERE NOT (document_id = ANY($1))`, seen)
	stats["deleted"] += int(tag.RowsAffected())
	writeJSON(w, 200, stats)
}

// ---------- Question answering ----------

type passage struct {
	documentID, title, section, content string
	similarity, score                   float64
}

type citation struct {
	DocumentID string `json:"documentId"`
	Title      string `json:"title"`
	Section    string `json:"section"`
}

type answer struct {
	Found     bool       `json:"found"`
	Answer    string     `json:"answer"`
	Citations []citation `json:"citations"`
}

func (s *server) ask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var q struct {
		Question string   `json:"question"`
		Groups   []string `json:"groups"`
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil || strings.TrimSpace(q.Question) == "" || len(q.Groups) == 0 {
		writeJSON(w, 400, map[string]string{"error": "question and groups are required"})
		return
	}
	notFoundAnswer := answer{Found: false, Answer: notFound, Citations: []citation{}}

	vectors, err := s.embed(ctx, []string{"search_query: " + q.Question})
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(ctx, retrievalSQL, toVector(vectors[0]), q.Question, q.Groups)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	var passages []passage
	best := 0.0
	for rows.Next() {
		var p passage
		if err := rows.Scan(&p.documentID, &p.title, &p.section, &p.content, &p.similarity, &p.score); err == nil {
			passages = append(passages, p)
			best = max(best, p.similarity)
		}
	}
	rows.Close()

	// Refuse without calling the model when nothing is relevant enough.
	if len(passages) == 0 || best < s.cfg.minSimilarity {
		writeJSON(w, 200, notFoundAnswer)
		return
	}

	var sb strings.Builder
	sb.WriteString("Passages:\n")
	for i, p := range passages {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		fmt.Fprintf(&sb, "[%d] %s", i+1, p.content)
	}
	fmt.Fprintf(&sb, "\n\nQuestion: %s", q.Question)

	var chat struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	err = s.postJSON(ctx, "/api/chat", map[string]any{
		"model":   s.cfg.chatModel,
		"stream":  false,
		"options": map[string]any{"temperature": 0},
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": sb.String()},
		},
	}, &chat)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	text := strings.TrimSpace(chat.Message.Content)
	if strings.Contains(text, "NOT_FOUND") {
		writeJSON(w, 200, notFoundAnswer)
		return
	}

	// Citations: passages referenced as [n], de-duplicated, in order of first mention.
	var cites []citation
	seen := map[string]bool{}
	for _, m := range citationRe.FindAllStringSubmatch(text, -1) {
		i, _ := strconv.Atoi(m[1])
		if i < 1 || i > len(passages) {
			continue
		}
		p := passages[i-1]
		if key := p.documentID + "|" + p.section; !seen[key] {
			seen[key] = true
			cites = append(cites, citation{p.documentID, p.title, p.section})
		}
	}
	if len(cites) == 0 {
		cites = []citation{{passages[0].documentID, passages[0].title, passages[0].section}}
	}
	writeJSON(w, 200, answer{Found: true, Answer: text, Citations: cites})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	minSim, _ := strconv.ParseFloat(env("MIN_SIMILARITY", "0.55"), 64)
	cfg := config{
		ollamaURL:     os.Getenv("OLLAMA_URL"),
		embedModel:    env("EMBED_MODEL", "nomic-embed-text"),
		chatModel:     env("CHAT_MODEL", "qwen2.5:3b"),
		docsDir:       env("DOCS_DIR", "/docs"),
		minSimilarity: minSim,
	}
	db, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	s := &server{db: db, cfg: cfg, http: &http.Client{Timeout: 5 * time.Minute}}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", s.ingest)
	mux.HandleFunc("POST /ask", s.ask)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	addr := ":" + env("PORT", "8080")
	slog.Info("rag service listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
