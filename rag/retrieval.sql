-- Hybrid retrieval used by every language implementation.
-- $1 = question embedding as text, e.g. [0.12,-0.03,...], $2 = question text, $3 = the user's groups (text[])
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
       COALESCE(s.similarity, 0) AS similarity,
       COALESCE(1.0 / (60 + s.rnk), 0) + COALESCE(1.0 / (60 + k.rnk), 0) AS score
FROM chunks c
LEFT JOIN semantic s ON s.id = c.id
LEFT JOIN keyword  k ON k.id = c.id
WHERE s.id IS NOT NULL OR k.id IS NOT NULL
ORDER BY score DESC
LIMIT 6;
