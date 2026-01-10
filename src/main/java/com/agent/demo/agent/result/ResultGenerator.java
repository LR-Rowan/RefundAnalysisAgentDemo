package com.agent.demo.agent.result;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.llm.OpenAIClient;
import com.agent.demo.llm.ResponsesTextExtractor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * LLM 非流式 → 严格 JSON
 */
@Service
public class ResultGenerator {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final OpenAIClient openAIClient;


    public ResultGenerator(OpenAIClient openAIClient) {
        this.openAIClient = openAIClient;
    }

    public Mono<AgentResult> generate(AgentContext ctx, List<?> toolResultsJsonSerializable) {
        return Mono.fromCallable(() -> mapper.writeValueAsString(toolResultsJsonSerializable))
                .flatMap(toolsJson -> openAIClient.callOnce(buildPrompt(ctx, toolsJson)))
                .map(ResponsesTextExtractor::extractOutputText)
                .map(this::extractJsonBlock)
                .map(String::trim)
                .map(this::parseResult);
    }

    private AgentResult parseResult(String json) {
        try {
            return mapper.readValue(json, AgentResult.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid_result_json: " + e.getMessage() + " raw=" + json, e);
        }
    }

    private String buildPrompt(AgentContext ctx, String toolsJson) {
        return """
        你是电商运营分析助手。你必须只输出一个 JSON，并且必须包裹在 <json> 与 </json> 标签内。
        除 <json>...</json> 外，禁止输出任何字符（包括空格、换行、Markdown）。

        请严格按以下 JSON Schema 输出（字段必须齐全）：
        {
          "storeId": "string",
          "windowDays": number,
          "keyFindings": ["string"],
          "rootCauses": [{"title":"string","impact":"string","evidence":"string"}],
          "actions": [{"title":"string","owner":"string","priority":"P0|P1|P2","eta":"string","howToVerify":"string"}],
          "evidence": {"tools":[...]}
        }

        输入：
        storeId=%s
        windowDays=%d
        query=%s

        工具结果（JSON）：
        %s

        现在输出：
        """.formatted(ctx.storeId(), ctx.windowDays(), ctx.query(), toolsJson);
    }

    private String extractJsonBlock(String s) {
        if (s == null) return "";
        int a = s.indexOf("<json>");
        int b = s.indexOf("</json>");
        if (a >= 0 && b > a) return s.substring(a + 6, b).trim();
        return s.trim();
    }
}
