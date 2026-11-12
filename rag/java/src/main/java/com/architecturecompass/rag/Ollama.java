package com.architecturecompass.rag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Minimal Ollama client: embeddings and chat. */
@Component
class Ollama {

    record EmbedResponse(List<List<Double>> embeddings) {}
    record ChatMessage(String role, String content) {}
    record ChatResponse(ChatMessage message) {}

    static final String EMBED_MODEL = env("EMBED_MODEL", "nomic-embed-text");
    static final String CHAT_MODEL = env("CHAT_MODEL", "qwen2.5:3b");

    private final RestClient http;

    Ollama() {
        SimpleClientHttpRequestFactory timeouts = new SimpleClientHttpRequestFactory();
        timeouts.setReadTimeout(Duration.ofMinutes(5));
        http = RestClient.builder().baseUrl(System.getenv("OLLAMA_URL")).requestFactory(timeouts).build();
    }

    List<List<Double>> embed(List<String> texts) {
        return http.post().uri("/api/embed")
                .body(Map.of("model", EMBED_MODEL, "input", texts))
                .retrieve().body(EmbedResponse.class).embeddings();
    }

    String chat(String system, String user) {
        return http.post().uri("/api/chat")
                .body(Map.of(
                        "model", CHAT_MODEL,
                        "stream", false,
                        "options", Map.of("temperature", 0),
                        "messages", List.of(new ChatMessage("system", system), new ChatMessage("user", user))))
                .retrieve().body(ChatResponse.class).message().content();
    }

    /** pgvector's text format: [0.1,0.2,...] */
    static String toVector(List<Double> v) {
        return v.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
    }

    static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
}
