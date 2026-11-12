package com.architecturecompass.rag;

import java.util.List;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * RAG service (Java): ingests policy documents into Postgres + pgvector and answers
 * questions with hybrid retrieval, permission filtering, citations and refusal.
 */
@SpringBootApplication
@RestController
public class RagApplication {

    record Question(String question, List<String> groups) {}

    private final Ingestor ingestor;
    private final Answerer answerer;

    RagApplication(Ingestor ingestor, Answerer answerer) {
        this.ingestor = ingestor;
        this.answerer = answerer;
    }

    public static void main(String[] args) {
        SpringApplication.run(RagApplication.class, args);
    }

    @PostMapping("/ingest")
    Map<String, Integer> ingest() throws Exception {
        return ingestor.ingest();
    }

    @PostMapping("/ask")
    ResponseEntity<?> ask(@RequestBody(required = false) Question q) {
        if (q == null || q.question() == null || q.question().isBlank() || q.groups() == null || q.groups().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "question and groups are required"));
        }
        return ResponseEntity.ok(answerer.ask(q.question(), q.groups()));
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "ok");
    }

}
