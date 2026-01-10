package com.agent.demo.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ResponsesTextExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String extractOutputText(String responseJson) {
        if (responseJson == null || responseJson.isBlank()) return "";

        try {
            JsonNode root = MAPPER.readTree(responseJson);

            // 1) 有些场景会直接有 output_text 字段
            JsonNode direct = root.path("output_text");
            if (direct.isTextual() && !direct.asText().isBlank()) {
                return direct.asText();
            }

            // 2) 兼容 root.response.*
            JsonNode resp = root.path("response");
            JsonNode respDirect = resp.path("output_text");
            if (respDirect.isTextual() && !respDirect.asText().isBlank()) {
                return respDirect.asText();
            }

            // 3) ✅ 关键：遍历 output[].content[] 找 type=output_text 的 text
            JsonNode output = root.path("output");
            if (output.isArray()) {
                for (JsonNode item : output) {
                    JsonNode content = item.path("content");
                    if (content.isArray()) {
                        for (JsonNode c : content) {
                            String type = c.path("type").asText("");
                            if ("output_text".equals(type)) {
                                String text = c.path("text").asText("");
                                if (!text.isBlank()) return text;
                            }
                        }
                    }
                }
            }

            // 4) 再兜底：如果 output 在 root.response.output
            JsonNode respOutput = resp.path("output");
            if (respOutput.isArray()) {
                for (JsonNode item : respOutput) {
                    JsonNode content = item.path("content");
                    if (content.isArray()) {
                        for (JsonNode c : content) {
                            String type = c.path("type").asText("");
                            if ("output_text".equals(type)) {
                                String text = c.path("text").asText("");
                                if (!text.isBlank()) return text;
                            }
                        }
                    }
                }
            }

            // 最后兜底：返回原文便于定位
            return responseJson;

        } catch (Exception e) {
            return responseJson;
        }
    }
}
