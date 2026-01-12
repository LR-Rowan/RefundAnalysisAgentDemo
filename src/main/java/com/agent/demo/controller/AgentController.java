package com.agent.demo.controller;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.agent.AgentEvent;
import com.agent.demo.agent.AgentOrchestrator;
import com.agent.demo.agent.result.ResultStore;
import com.agent.demo.config.AgentTimeoutProperties;
import com.agent.demo.dto.RunRequest;
import com.agent.demo.infra.ConcurrencyLimiter;
import com.agent.demo.metrics.AgentMetrics;
import com.agent.demo.trace.TraceKeys;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
     * - seq: 单次请求内事件递增序号（从 0 开始）（仅业务事件；心跳 comment 不计入）
     * - ts: 服务器发送时间（epoch millis）
     * - durationMs: 从本次 run 开始到当前事件的耗时
     * - stage: 粗粒度阶段（planner/tool/summarizer/result/done/error/unknown）
     * - data: payload（自动识别 JSON -> JsonNode，否则 String / Map）
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

    /**
     * 统一错误负载（HTTP/SSE 都可复用；这里先放 Controller 内避免工程里重复类名）
     */
    private record AgentErrorPayload(
            String code,
            String message,
            String stage
    ) {}

    @Autowired
    private AgentOrchestrator agentOrchestrator;

    @Autowired
    private ResultStore resultStore;

    @Autowired
    private ConcurrencyLimiter limiter;

    @Autowired
    private AgentTimeoutProperties timeoutProps;

    @Autowired
    private AgentMetrics metrics;

    @Value("${agent.sse.heartbeatSeconds:10}")
    private long heartbeatSeconds;

    @Value("${agent.sse.heartbeatComment:ping}")
    private String heartbeatComment;

    /**
     * produces = TEXT_EVENT_STREAM: 告诉 Spring 返回的是 SSE
     */
    @PostMapping(value = "/run", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> run(
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestParam(value = "legacy", required = false, defaultValue = "false") boolean legacy,
            @RequestParam(value = "debugHoldMs", required = false, defaultValue = "0") long debugHoldMs,
            @Valid @RequestBody RunRequest request
    ) {
        int windowDays = Objects.isNull(request.windowDays()) ? WINDOW_DAYS : request.windowDays();
        AgentContext ctx = new AgentContext(request.storeId(), request.query(), windowDays);

        return Flux.defer(() -> {
            ConcurrencyLimiter.Permit permit = limiter.tryAcquire(ctx.storeId());
            if (!permit.acquired()) {
                metrics.rateLimited(permit.denyReason());
                return Flux.error(new ResponseStatusException(
                        HttpStatus.TOO_MANY_REQUESTS,
                        "rate_limited:" + permit.denyReason()
                ));
            }

            metrics.inflightInc();
            io.micrometer.core.instrument.Timer.Sample runSample = metrics.runStart();
            final long startMillis = System.currentTimeMillis();
            Flux<AgentEvent> source = agentOrchestrator.run(ctx, traceId)
                    .contextWrite(c -> c.put(TraceKeys.STORE_ID, ctx.storeId()).put(TraceKeys.WINDOW_DAYS, windowDays));

            // -------- 1) 只构建“业务 SSE 流”（不含 heartbeat） --------
            Flux<ServerSentEvent<String>> business;
            if (legacy) {
                business = source.map(evt -> ServerSentEvent.<String>builder()
                        .event(evt.type())
                        .data(evt.payload())
                        .build());
            } else {
                business = source
                        .index()
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
                                data = "{\"resultId\":\"" + safe(evt.resultId()) +
                                        "\",\"traceId\":\"" + safe(evt.traceId()) +
                                        "\",\"seq\":" + seq +
                                        ",\"ts\":" + ts +
                                        ",\"durationMs\":" + durationMs +
                                        ",\"stage\":\"error\"" +
                                        ",\"data\":\"json_serialize_failed\"}";
                            }

                            return ServerSentEvent.<String>builder()
                                    .event(evt.type())
                                    .data(data)
                                    .build();
                        });
            }

            // Debug hold：用于压测/验收并发隔离，让连接占用更稳定（非阻塞）
            if (debugHoldMs > 0) {
                business = business.concatWith(Mono.delay(Duration.ofMillis(debugHoldMs)).flatMapMany(x -> Flux.empty()));
            }

            // -------- 2) run timeout 只作用在业务流（避免 heartbeat 刷新 timeout） --------
            Flux<ServerSentEvent<String>> guardedBusiness = business
                    .timeout(timeoutProps.getRun())
                    .doOnCancel(() -> {
                        // 业务侧断连（更靠近真实 cancel）
                        // 你也可以这里加一条日志：log.warn("sse_client_disconnected");
                    });

            // -------- 3) heartbeat 只在业务流存活期间发送，业务结束后 heartbeat 自动停止 --------
            // 心跳间隔建议 10s（可配置），这里写死示例；你可以替换成配置项
            Flux<ServerSentEvent<String>> heartbeat = Flux.interval(Duration.ofSeconds(10))
                    .map(t -> ServerSentEvent.<String>builder().comment("ping").build())
                    .takeUntilOther(guardedBusiness.ignoreElements());

            // 合并后：业务结束 => heartbeat 也结束 => SSE 连接自然 close
            Flux<ServerSentEvent<String>> out = Flux.merge(heartbeat, guardedBusiness);

            // doFinally 绑在最终 out 上，确保 complete/cancel/error 都释放资源
            return out.doFinally(sig -> {
                String outcome = switch (sig) {
                    case ON_COMPLETE -> "success";
                    case CANCEL -> "cancel";
                    case ON_ERROR -> "error";
                    default -> "unknown";
                };
                metrics.runEnd(runSample, outcome);
                metrics.inflightDec();
                permit.release();
            });
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

    private ServerSentEvent<String> buildEnvelopeEvent(
            String event,
            String resultId,
            String traceId,
            long seq,
            long startMillis,
            String stage,
            Object dataObj) {
        long ts = System.currentTimeMillis();
        long durationMs = ts - startMillis;

        SseEnvelope env = new SseEnvelope(
                resultId,
                traceId,
                seq,
                ts,
                durationMs,
                stage,
                dataObj
        );
        String data;
        try {
            data = mapper.writeValueAsString(env);
        } catch (Exception e) {
            data = "{\"resultId\":\"" + safe(resultId) +
                    "\",\"traceId\":\"" + safe(traceId) +
                    "\",\"seq\":" + seq +
                    ",\"ts\":" + ts +
                    ",\"durationMs\":" + durationMs +
                    ",\"stage\":\"error\"" +
                    ",\"data\":\"json_serialize_failed\"}";
        }

        return ServerSentEvent.<String>builder()
                .event(event)
                .data(data)
                .build();
    }

    private static boolean isTimeout(Throwable ex) {
        if (ex == null) return false;
        if (ex instanceof TimeoutException) return true;
        String n = ex.getClass().getName();
        // reactor 的超时异常类名可能不同版本有差异
        return n != null && n.toLowerCase().contains("timeout");
    }

    private static String safeMsg(Throwable ex) {
        if (ex == null) return "unexpected error";
        String msg = ex.getMessage();
        if (msg == null || msg.isBlank()) return ex.getClass().getSimpleName();
        return msg.length() > 300 ? msg.substring(0, 300) : msg;
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
