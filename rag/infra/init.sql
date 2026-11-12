-- Shared schema for every language implementation.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE chunks (
    id              BIGSERIAL PRIMARY KEY,
    document_id     TEXT         NOT NULL,      -- file name without .md
    chunk_index     INT          NOT NULL,
    title           TEXT         NOT NULL,
    section         TEXT         NOT NULL,
    content         TEXT         NOT NULL,      -- "Title > Section" + section text
    allowed_groups  TEXT[]       NOT NULL,      -- copied from the document's access list
    content_hash    TEXT         NOT NULL,      -- sha256 of model + groups + content
    embedding_model TEXT         NOT NULL,
    embedding       vector(768)  NOT NULL,      -- nomic-embed-text produces 768 dimensions
    content_tsv     tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX chunks_keywords  ON chunks USING gin (content_tsv);
CREATE INDEX chunks_groups    ON chunks USING gin (allowed_groups);
