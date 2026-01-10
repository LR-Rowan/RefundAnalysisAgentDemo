package com.agent.demo.tools;

import com.agent.demo.agent.AgentContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class LogisticsDelayTool implements Tool {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String name() {
        return "logistics_delay";
    }

    @Override
    public Mono<ToolResult> execute(AgentContext ctx) {
        return Mono.fromCallable(() -> compute(ctx))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private ToolResult compute(AgentContext ctx) throws Exception {
        ClassPathResource resource = new ClassPathResource("data/logistics.json");
        if (!resource.exists()) {
            throw new IllegalStateException("logistics.json not found at classpath:data/logistics.json");
        }

        String json = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        JsonNode node = MAPPER.readTree(json);

        String storeId = node.path("storeId").asText("");
        if (!storeId.isEmpty() && !storeId.equals(ctx.storeId())) {
            throw new IllegalArgumentException("storeId mismatch: file=" + storeId + ", request=" + ctx.storeId());
        }

        int windowDays = node.path("windowDays").asInt(ctx.windowDays());
        double delayedRate = node.path("delayedRate").asDouble(0.0);

        // Top carriers / cities 保持原样输出即可，前端/LLM 都好用
        JsonNode topCarriers = node.path("topCarriers");
        JsonNode topCities = node.path("topCities");

        String summary = "物流延迟率=" + round2(delayedRate * 100) + "%（窗口=" + windowDays + "天）";

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("storeId", ctx.storeId());
        metrics.put("windowDays", windowDays);
        metrics.put("delayedRate", delayedRate);
        metrics.put("topCarriers", MAPPER.convertValue(topCarriers, Object.class));
        metrics.put("topCities", MAPPER.convertValue(topCities, Object.class));

        return new ToolResult(name(), summary, metrics);
    }

    private static double round2(double x) {
        return Math.round(x * 100.0) / 100.0;
    }
}
