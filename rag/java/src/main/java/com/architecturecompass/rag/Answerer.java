package com.architecturecompass.rag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Hybrid retrieval, permission filtering, refusal, generation and citations. */
@Component
class Answerer {

    record Citation(String documentId, String title, String section) {}
    record Answer(boolean found, String answer, List<Citation> citations) {}
    private record Passage(String documentId, String title, String section, String content, double similarity) {}

    static final String NOT_FOUND = "I couldn't find this in the policy documents.";

    static final String SYSTEM_PROMPT = """
            You answer employee questions using only the numbered policy passages provided.
            Write the answer as one to three complete sentences, and put the number of the passage you used in square brackets at the end of each sentence.
            Example of the format: Employees get 10 days of sick leave per year [3].
            If the passages do not contain the answer, reply with exactly NOT_FOUND.""";

    // Parameters, in order: vector, vector, groups, vector, question text, groups.
    static final String RETRIEVAL_SQL = """
            WITH semantic AS (
                SELECT id,
                       1 - (embedding <=> ?::vector) AS similarity,
                       row_number() OVER (ORDER BY embedding <=> ?::vector) AS rnk
                FROM chunks
                WHERE allowed_groups && ?::text[]
                ORDER BY embedding <=> ?::vector
                LIMIT 40
            ),
            keyword AS (
                SELECT id,
                       row_number() OVER (ORDER BY ts_rank_cd(content_tsv, q) DESC) AS rnk
                FROM chunks, websearch_to_tsquery('english', ?) AS q
                WHERE content_tsv @@ q AND allowed_groups && ?::text[]
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
            LIMIT 6""";

    private static final Pattern CITATION = Pattern.compile("\\[(\\d+)]");

    private final JdbcTemplate jdbc;
    private final Ollama ollama;
    private final double minSimilarity = Double.parseDouble(Ollama.env("MIN_SIMILARITY", "0.55"));

    Answerer(JdbcTemplate jdbc, Ollama ollama) {
        this.jdbc = jdbc;
        this.ollama = ollama;
    }

    Answer ask(String question, List<String> groupList) {
        Answer notFound = new Answer(false, NOT_FOUND, List.of());
        String vec = Ollama.toVector(ollama.embed(List.of("search_query: " + question)).get(0));
        String[] groups = groupList.toArray(String[]::new);

        List<Passage> rows = jdbc.query(RETRIEVAL_SQL,
                (rs, i) -> new Passage(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDouble(5)),
                vec, vec, groups, vec, question, groups);

        // Refuse without calling the model when nothing is relevant enough.
        if (rows.isEmpty() || rows.stream().mapToDouble(Passage::similarity).max().orElse(0) < minSimilarity) {
            return notFound;
        }

        StringBuilder user = new StringBuilder("Passages:\n");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) user.append("\n\n");
            user.append('[').append(i + 1).append("] ").append(rows.get(i).content());
        }
        user.append("\n\nQuestion: ").append(question);

        String answer = ollama.chat(SYSTEM_PROMPT, user.toString()).strip();
        if (answer.contains("NOT_FOUND")) return notFound;

        // Citations: passages referenced as [n], de-duplicated, in order of first mention.
        List<Citation> citations = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Matcher m = CITATION.matcher(answer);
        while (m.find()) {
            int i = Integer.parseInt(m.group(1)) - 1;
            if (i < 0 || i >= rows.size()) continue;
            Passage p = rows.get(i);
            if (seen.add(p.documentId() + "|" + p.section())) {
                citations.add(new Citation(p.documentId(), p.title(), p.section()));
            }
        }
        if (citations.isEmpty()) {
            Passage p = rows.get(0);
            citations.add(new Citation(p.documentId(), p.title(), p.section()));
        }
        return new Answer(true, answer, citations);
    }
}
