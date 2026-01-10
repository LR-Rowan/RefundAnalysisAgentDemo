package com.agent.demo.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Responses API 的返回 JSON 里，真正文本通常不在根节点，你不能直接用 bodyToMono(String.class) 当作最终 plan 文本。
 * 所以我们还要加一个解析函数：从 response JSON 中提取 output_text
 */
public class ResponsesTextExtractor {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 从 /v1/responses 的非流式返回中提取输出文本（尽量稳健）。
     */
    public static String extractOutputText(String responseJson) {
        try {
            JsonNode root = MAPPER.readTree(responseJson);

            // 常见：root.output_text（如果存在就直接用）
            JsonNode outputText = root.path("output_text");
            if (outputText.isTextual() && !outputText.asText().isBlank()) {
                return outputText.asText();
            }

            // 兜底：root.response.output_text（你流式事件里有 response 字段）
            JsonNode resp = root.path("response");
            JsonNode respOutputText = resp.path("output_text");
            if (respOutputText.isTextual() && !respOutputText.asText().isBlank()) {
                return respOutputText.asText();
            }

            // 再兜底：遍历 output 数组，拼 content.text（不同模型/版本可能差异）
            JsonNode output = resp.path("output");
            if (output.isArray()) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode item : output) {
                    JsonNode content = item.path("content");
                    if (content.isArray()) {
                        for (JsonNode c : content) {
                            JsonNode text = c.path("text");
                            if (text.isTextual()) sb.append(text.asText());
                        }
                    }
                }
                String t = sb.toString();
                if (!t.isBlank()) return t;
            }

            // 实在提取不到，返回原始 JSON 以便定位
            return responseJson;
        } catch (Exception e) {
            return responseJson;
        }
    }
}
