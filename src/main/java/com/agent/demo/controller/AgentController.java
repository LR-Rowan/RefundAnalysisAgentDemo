package com.agent.demo.controller;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.agent.AgentEvent;
import com.agent.demo.agent.AgentOrchestrator;
import com.agent.demo.agent.result.ResultStore;
import com.agent.demo.config.AgentTimeoutProperties;
import com.agent.demo.dto.RunRequest;
import com.agent.demo.infra.ConcurrencyLimiter;
import com.agent.demo.trace.TraceKeys;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

/**
 * SSE Controller
 * <p>
 * Controller 里的方法不会阻塞线程等结果，而是立即返回一个 Flux
 */
@RestController
@RequestMapping("/agent")
public class AgentController {

    private static final int WINDOW_DAYS = 7;
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * SSE 可观测增强：
     * - seq: 单次请求内事件递增序号（从 0 开始）
     * - ts: 服务器发送时间（epoch millis）
     * - durationMs: 从本次 run 开始到当前事件的耗时
     * - stage: 粗粒度阶段（planner/tool/summarizer/result/done/error/unknown）
     * - data: payload（自动识别 JSON -> JsonNode，否则 String）
     */
    private record SseEnvelope(
            String resultId,
            String traceId,
            long seq,
            long ts,
            long durationMs,
            String stage,
            Object data
    ) {}

    @Autowired
    private AgentOrchestrator agentOrchestrator;

    @Autowired
    private ResultStore resultStore;

    @Autowired
    private ConcurrencyLimiter limiter;

    @Autowired
    private AgentTimeoutProperties timeoutProps;

    /**
     * produces = TEXT_EVENT_STREAM: 告诉 Spring 返回的是 SSE
     */
    @PostMapping(
            value = "/run",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<String>> run(
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestParam(value = "legacy", required = false, defaultValue = "false") boolean legacy,
            @RequestParam(value = "debugHoldMs", required = false, defaultValue = "0") long debugHoldMs,
            @Valid @RequestBody RunRequest request
    ) {
        int windowDays = Objects.isNull(request.windowDays()) ? WINDOW_DAYS : request.windowDays();
        AgentContext ctx = new AgentContext(request.storeId(), request.query(), windowDays);

        // defer: 确保只有真正订阅时才占用 permit
        return Flux.defer(() -> {
            ConcurrencyLimiter.Permit permit = limiter.tryAcquire(ctx.storeId());
            if (!permit.acquired()) {
                return Flux.error(new ResponseStatusException(
                        HttpStatus.TOO_MANY_REQUESTS,
                        "rate_limited:" + permit.denyReason()
                ));
            }

            final long startMillis = System.currentTimeMillis();
            Flux<AgentEvent> source = agentOrchestrator.run(ctx, traceId);
            // 把业务维度写入 Reactor Context，供 MDC/日志/下游使用
            source = source.contextWrite(c ->
                    c.put(TraceKeys.STORE_ID, ctx.storeId()).put(TraceKeys.WINDOW_DAYS, windowDays)
            );

            Flux<ServerSentEvent<String>> out;
            if (legacy) {
                // 旧协议：完全不包 envelope
                out = source.map(evt -> ServerSentEvent.<String>builder()
                        .event(evt.type())
                        .data(evt.payload())
                        .build());
            } else {
                // 新协议：带观测字段 + envelope
                out = source
                        .index() // 给每个事件一个 seq（0..n）
                        .map(tuple -> {
                            long seq = tuple.getT1();
                            AgentEvent evt = tuple.getT2();
                            long ts = System.currentTimeMillis();
                            long durationMs = ts - startMillis;

                            Object payloadObj = tryParseJson(evt.payload());
                            String stage = inferStage(evt, payloadObj);

                            SseEnvelope env = new SseEnvelope(
                                    evt.resultId(),
                                    evt.traceId(),
                                    seq,
                                    ts,
                                    durationMs,
                                    stage,
                                    payloadObj
                            );

                            String data;
                            try {
                                data = mapper.writeValueAsString(env);
                            } catch (Exception e) {
                                // 生产级容错：序列化失败不能打断 SSE stream
                                data = "{\"resultId\":\"" + safe(evt.resultId()) +
                                        "\",\"traceId\":\"" + safe(evt.traceId()) +
                                        "\",\"seq\":" + seq +
                                        ",\"ts\":" + ts +
                                        ",\"durationMs\":" + durationMs +
                                        ",\"stage\":\"error\"" +
                                        ",\"data\":\"json_serialize_failed\"}";
                            }

                            return ServerSentEvent.<String>builder()
                                    .event(evt.type()) // event 名仍然保持：status/tool/delta/result/result_meta...
                                    .data(data)
                                    .build();
                        });
            }

            // Debug hold：用于压测/验收并发隔离，让连接占用更稳定（非阻塞）
            // debugHoldMs=0 默认不生效
            if (debugHoldMs > 0) {
                out = out.concatWith(Mono.delay(Duration.ofMillis(debugHoldMs)).flatMapMany(x -> Flux.empty()));
            }

            // 1) timeout 绑定在最终 out 上，避免外层 timeout 截断导致 permit 不释放
            // 2) doFinally 也绑定在最终 out 上，确保 cancel/timeout/complete 都释放
            return out
                    .timeout(timeoutProps.getRun())
                    .doFinally(sig -> permit.release());
        });
    }

    /**
     * 下载接口, 浏览器/curl 直接能拿到 JSON
     */
    @GetMapping(value = "/results/{resultId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> downloadResult(@PathVariable("resultId") String resultId) {
        return Mono.fromCallable(() -> resultStore.loadRaw(resultId))
                .map(body -> ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(body)
                )
                .onErrorResume(ex -> Mono.just(
                        ResponseEntity.status(404)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body("{\"error\":\"not_found\",\"message\":\"" + ex.getMessage().replace("\"", "\\\"") + "\"}")
                ));
    }

    /**
     * 如果 evt.payload() 是 JSON → 解析成 JSON 对象，若不是则保持为 String
     * - 支持 <json>...</json> 强约束格式
     */
    private Object tryParseJson(String payload) {
        if (Objects.isNull(payload) || payload.isBlank()) {
            return payload;
        }
        String s = payload.trim();

        // 支持 <json>...</json>
        if (s.startsWith("<json>") && s.endsWith("</json>")) {
            s = s.substring(6, s.length() - 7).trim();
        }

        if (!(s.startsWith("{") || s.startsWith("["))) {
            // 明显不是 JSON
            return payload;
        }
        try {
            return mapper.readTree(s); // 返回 JsonNode
        } catch (Exception e) {
            // 不是合法 JSON，当普通字符串
            return payload;
        }
    }

    /**
     * 推断 stage：尽量基于 event.type + payload 内容做粗粒度分类
     * 生产级要点：稳定、可扩展、不要过度耦合 payload 结构
     */
    private String inferStage(AgentEvent evt, Object payloadObj) {
        String type = evt.type();
        String payload = evt.payload() == null ? "" : evt.payload();

        // 1) 直接用 event type 判断（兼容 result_meta / resultMeta）
        if ("result".equals(type) || "result_meta".equals(type) || "resultMeta".equals(type)) return "result";
        if ("delta".equals(type)) return "summarizer";
        if ("tool".equals(type)) return "tool";

        // 2) status 需要看 payload
        if ("status".equals(type)) {
            String p = payload.trim();

            if (p.startsWith("<json>") && p.endsWith("</json>")) {
                p = p.substring(6, p.length() - 7).trim();
            }

            if (p.startsWith("planning")) return "planner";
            if (p.startsWith("tool_running:")) return "tool";
            if (p.startsWith("tool_error:")) return "tool";
            if (p.startsWith("tool_timeout:")) return "tool";
            if (p.startsWith("summarizing")) return "summarizer";
            if (p.startsWith("result_generating")) return "result";
            if (p.startsWith("llm_error")) return "summarizer";
            if (p.startsWith("llm_idle_timeout")) return "summarizer";
            if (p.startsWith("llm_max_timeout")) return "summarizer";
            if (p.startsWith("done")) return "done";
            if (p.startsWith("run_timeout")) return "error";

            // 如果 status 是 JSON（比如 {"stage":"..."}），优先读 stage 字段
            if (payloadObj instanceof JsonNode node) {
                JsonNode stage = node.get("stage");
                if (stage != null && stage.isTextual()) return stage.asText();
                // 兼容 {"type":"llm_idle_timeout"} 这种结构
                JsonNode typeNode = node.get("type");
                if (typeNode != null && typeNode.isTextual()) {
                    String t = typeNode.asText();
                    if (t.startsWith("llm_")) return "summarizer";
                }
            }
        }
        return "unknown";
    }

    // 避免 null/引号炸裂
    private static String safe(String s) {
        if (Objects.isNull(s)) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
