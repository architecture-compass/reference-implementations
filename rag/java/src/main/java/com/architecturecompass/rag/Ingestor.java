package com.architecturecompass.rag;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Reads markdown documents, chunks them by section, and embeds new or changed chunks. */
@Component
class Ingestor {

    private record Chunk(String section, String content) {}
    private record Document(String title, List<String> groups, String body) {}

    private static final int MAX_CHUNK_CHARS = 1500;
    private static final Pattern HEADER = Pattern.compile("(?s)^---\\n(.*?)\\n---\\n(.*)$");
    private static final Pattern SECTION = Pattern.compile("(?m)^## ");

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Ollama ollama;
    private final Path docsDir = Path.of(Ollama.env("DOCS_DIR", "/docs"));

    Ingestor(JdbcTemplate jdbc, TransactionTemplate tx, Ollama ollama) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.ollama = ollama;
    }

    static Document parse(String text) {
        Matcher m = HEADER.matcher(text);
        if (!m.matches()) throw new IllegalArgumentException("document has no header");
        Map<String, String> header = new HashMap<>();
        for (String line : m.group(1).split("\n")) {
            int i = line.indexOf(':');
            if (i > 0) header.put(line.substring(0, i).trim(), line.substring(i + 1).trim());
        }
        List<String> groups = Arrays.stream(header.get("groups").split(","))
                .map(String::trim).filter(g -> !g.isEmpty()).toList();
        return new Document(header.get("title"), groups, m.group(2));
    }

    /** Split text longer than MAX_CHUNK_CHARS on paragraph boundaries. */
    static List<String> splitLong(String text) {
        List<String> parts = new ArrayList<>();
        String current = "";
        for (String para : text.split("\n\n")) {
            if (!current.isEmpty() && current.length() + para.length() + 2 > MAX_CHUNK_CHARS) {
                parts.add(current);
                current = para;
            } else {
                current = current.isEmpty() ? para : current + "\n\n" + para;
            }
        }
        if (!current.isEmpty()) parts.add(current);
        return parts;
    }

    /** One chunk per ## section; each chunk is prefixed with title and section. */
    static List<Chunk> chunk(String title, String body) {
        List<Chunk> chunks = new ArrayList<>();
        String[] blocks = SECTION.split(body);
        for (int b = 1; b < blocks.length; b++) {
            int nl = blocks[b].indexOf('\n');
            String section = (nl < 0 ? blocks[b] : blocks[b].substring(0, nl)).trim();
            String text = nl < 0 ? "" : blocks[b].substring(nl + 1).trim();
            for (String part : splitLong(text)) {
                chunks.add(new Chunk(section, title + " > " + section + "\n\n" + part));
            }
        }
        return chunks;
    }

    static String sha256(String s) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
    }

    Map<String, Integer> ingest() throws Exception {
        Map<String, Integer> stats = new LinkedHashMap<>();
        for (String k : List.of("documents", "chunks", "embedded", "skipped", "deleted")) stats.put(k, 0);
        List<String> seen = new ArrayList<>();

        List<Path> files;
        try (Stream<Path> s = Files.list(docsDir)) {
            files = s.filter(p -> p.toString().endsWith(".md")).sorted().toList();
        }
        for (Path file : files) {
            String documentId = file.getFileName().toString().replaceFirst("\\.md$", "");
            Document doc = parse(Files.readString(file));
            List<Chunk> chunks = chunk(doc.title(), doc.body());
            stats.merge("documents", 1, Integer::sum);
            stats.merge("chunks", chunks.size(), Integer::sum);
            seen.add(documentId);

            Map<Integer, String> existing = new HashMap<>();
            jdbc.query("SELECT chunk_index, content_hash FROM chunks WHERE document_id = ?",
                    rs -> { existing.put(rs.getInt(1), rs.getString(2)); }, documentId);

            List<String> hashes = new ArrayList<>();
            List<Integer> changed = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                hashes.add(sha256(Ollama.EMBED_MODEL + "|" + String.join(",", doc.groups()) + "|" + chunks.get(i).content()));
                if (!hashes.get(i).equals(existing.get(i))) changed.add(i);
            }
            stats.merge("skipped", chunks.size() - changed.size(), Integer::sum);

            if (!changed.isEmpty()) {
                List<List<Double>> vectors = ollama.embed(
                        changed.stream().map(i -> "search_document: " + chunks.get(i).content()).toList());
                String[] groups = doc.groups().toArray(String[]::new);
                tx.executeWithoutResult(status -> {
                    for (int j = 0; j < changed.size(); j++) {
                        int i = changed.get(j);
                        jdbc.update("""
                                INSERT INTO chunks (document_id, chunk_index, title, section, content,
                                    allowed_groups, content_hash, embedding_model, embedding)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::vector)
                                ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                                    title = EXCLUDED.title, section = EXCLUDED.section,
                                    content = EXCLUDED.content, allowed_groups = EXCLUDED.allowed_groups,
                                    content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
                                    embedding = EXCLUDED.embedding, updated_at = now()""",
                                documentId, i, doc.title(), chunks.get(i).section(), chunks.get(i).content(),
                                groups, hashes.get(i), Ollama.EMBED_MODEL, Ollama.toVector(vectors.get(j)));
                    }
                });
                stats.merge("embedded", changed.size(), Integer::sum);
            }

            // Sections removed from this document.
            stats.merge("deleted", jdbc.update(
                    "DELETE FROM chunks WHERE document_id = ? AND chunk_index >= ?", documentId, chunks.size()),
                    Integer::sum);
        }

        // Documents removed from the folder.
        stats.merge("deleted", jdbc.update(
                "DELETE FROM chunks WHERE NOT (document_id = ANY(?))", (Object) seen.toArray(String[]::new)),
                Integer::sum);
        return stats;
    }
}
