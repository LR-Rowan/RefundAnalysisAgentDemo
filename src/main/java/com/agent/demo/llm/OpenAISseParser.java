package com.agent.demo.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * 只解析 output_text.delta，其它事件如 tool_call、usage 先忽略，接口可扩展
 */
public class OpenAISseParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Optional<JsonNode> parseJson(String line) {
        if (line == null) return Optional.empty();
        String s = line.trim();
        if (s.isEmpty() || s.equals("[DONE]")) return Optional.empty();

        // 兼容两种：data: {...} 或者 {...}
        if (s.startsWith("data:")) {
            s = s.substring(5).trim();
            if (s.isEmpty() || s.equals("[DONE]")) return Optional.empty();
        }

        // 不是 JSON 就忽略
        if (!s.startsWith("{")) return Optional.empty();

        try {
            return Optional.of(MAPPER.readTree(s));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<String> extractDelta(String line) {
        return parseJson(line).flatMap(node -> {
            String type = node.path("type").asText("");
            if ("response.output_text.delta".equals(type)) {
                String delta = node.path("delta").asText("");
                return delta.isEmpty() ? Optional.empty() : Optional.of(delta);
            }
            return Optional.empty();
        });
    }

    public static Optional<String> extractError(String line) {
        return parseJson(line).flatMap(node -> {
            if (!"error".equals(node.path("type").asText(""))) return Optional.empty();
            String code = node.at("/error/code").asText("");
            String msg  = node.at("/error/message").asText("");
            String text = (code.isEmpty() ? "" : ("[" + code + "] ")) + msg;
            return text.isEmpty() ? Optional.empty() : Optional.of(text);
        });
    }
}