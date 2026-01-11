package com.agent.demo.agent;

import com.agent.demo.agent.plan.Plan;
import com.agent.demo.agent.plan.Planner;
import com.agent.demo.agent.plan.ToolCall;
import com.agent.demo.agent.result.ResultFallBackBuilder;
import com.agent.demo.agent.result.ResultGenerator;
import com.agent.demo.agent.result.ResultStore;
import com.agent.demo.config.AgentTimeoutProperties;
import com.agent.demo.llm.OpenAIClient;
import com.agent.demo.llm.OpenAISseParser;
import com.agent.demo.metrics.AgentMetrics;
import com.agent.demo.tools.ToolRegistry;
import com.agent.demo.tools.ToolResult;
import com.agent.demo.trace.IdGenerators;
import com.agent.demo.trace.TraceKeys;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Agent - 编排器
 */
@Service
public class AgentOrchestrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentOrchestrator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ResultGenerator 的超时建议单独配置；先给一个稳定默认值（你也可以挪到 timeoutProps）
    private static final Duration RESULT_TIMEOUT = Duration.ofSeconds(60);

    private final Planner planner;
    private final ToolRegistry toolRegistry;
    private final OpenAIClient openAIClient;
    private final ResultGenerator resultGenerator;
    private final ResultStore resultStore;
    private final AgentTimeoutProperties timeoutProps;
    private final AgentMetrics metrics;

    public AgentOrchestrator(
            Planner planner,
            ToolRegistry toolRegistry,
            OpenAIClient openAIClient,
            ResultGenerator resultGenerator,
            ResultStore resultStore,
            AgentTimeoutProperties timeoutProps,
            AgentMetrics metrics
    ) {
        this.planner = planner;
        this.toolRegistry = toolRegistry;
        this.openAIClient = openAIClient;
        this.resultGenerator = resultGenerator;
        this.resultStore = resultStore;
        this.timeoutProps = timeoutProps;
        this.metrics = metrics;
    }

    public Flux<AgentEvent> run(AgentContext ctx, String incomingTraceId) {
        final String resultId = IdGenerators.newResultId();
        final String traceId = (incomingTraceId != null && !incomingTraceId.isBlank())
                ? incomingTraceId
                : IdGenerators.newTraceId();

        // run timer
        final var runSample = metrics.runStart();

        // tool 汇总结果：顺序执行当前没并发，但这里做成线程安全，避免未来改并发踩坑
        final List<ToolResult> collected = Collections.synchronizedList(new ArrayList<>());

        // 1) planner（cache：保证只执行一次）
        Mono<Plan> planMono = planner.plan(ctx)
                .onErrorResume(ex -> {
                    LOGGER.warn("planner_failed, fallback default plan. err={}", ex.toString());
                    return Mono.just(defaultPlan(ctx));
                })
                .cache();

        Flux<AgentEvent> start = Flux.just(AgentEvent.status("planning"));
        Flux<AgentEvent> planEvent = planMono.map(this::toPlanEvent).flux();

        // 2) tools（concatMap：严格顺序）
        Flux<AgentEvent> toolsFlow = planMono.flatMapMany(plan ->
                Flux.fromIterable(plan.tools())
                        .concatMap(call -> runOneTool(ctx, call, collected))
        );

        // 3) summarizer（LLM stream + delta 合并 + idle/max timeout）
        Flux<AgentEvent> summarizingStart = Flux.just(AgentEvent.status("summarizing"));
        Flux<AgentEvent> summarizingFinal = buildSummarizing(ctx, collected);

        // 4) result generator + store
        Flux<AgentEvent> resultStart = Flux.just(AgentEvent.status("result_generating"));
        Flux<AgentEvent> resultFlow = buildResultFlow(ctx, collected, resultId);

        Flux<AgentEvent> end = Flux.just(AgentEvent.status("done"));

        Flux<AgentEvent> mainFlow = Flux.concat(
                start,
                planEvent,
                toolsFlow,
                summarizingStart,
                summarizingFinal,
                resultStart,
                resultFlow,
                end
        );

        // 5) run-level timeout：统一兜底，保证落盘可下载
        Flux<AgentEvent> withTimeout = mainFlow
                .timeout(timeoutProps.getRun())
                .onErrorResume(TimeoutException.class, ex -> {
                    metrics.timeout("run");
                    return runTimeoutFallback(ctx, collected, resultId, timeoutProps.getRun());
                });

        return withTimeout
                .doOnCancel(() -> {
                    // client cancel：通常是浏览器断开 / curl ctrl+c
                    LOGGER.warn("client_disconnected");
                    // 这里不 stop runSample；让 doFinally 统一处理
                })
                .doFinally(sig -> {
                    // run outcome
                    String outcome = switch (sig) {
                        case ON_COMPLETE -> "success";
                        case CANCEL -> "cancel";
                        case ON_ERROR -> "error";
                        default -> "unknown";
                    };
                    // 注意：run_timeout 在上面被兜底成正常完成，sig 可能是 ON_COMPLETE
                    // 口径修复建议在 Controller 侧更准确（按事件内容判断 timeout）
                    metrics.runEnd(runSample, outcome);

                    LOGGER.info("run_finally signal={}", sig);
                })
                .transform(this::stampTrace)
                .contextWrite(c -> c.put(TraceKeys.RESULT_ID, resultId).put(TraceKeys.TRACE_ID, traceId));
    }

    // ------------------------- planner -------------------------

    private Plan defaultPlan(AgentContext ctx) {
        return new Plan(List.of(
                new ToolCall("refund_rate", Map.of("windowDays", ctx.windowDays(), "storeId", ctx.storeId())),
                new ToolCall("logistics_delay", Map.of("windowDays", ctx.windowDays(), "storeId", ctx.storeId()))
        ));
    }

    private AgentEvent toPlanEvent(Plan plan) {
        try {
            String planJson = MAPPER.writeValueAsString(plan);
            return AgentEvent.tool("{\"toolName\":\"planner\",\"summary\":\"plan_selected\",\"metrics\":" + planJson + "}");
        } catch (Exception e) {
            return AgentEvent.tool("{\"toolName\":\"planner\",\"summary\":\"plan_selected\"}");
        }
    }

    // ------------------------- tools -------------------------

    private Flux<AgentEvent> runOneTool(AgentContext ctx, ToolCall call, List<ToolResult> collected) {
        final String toolName = call.name();
        final int windowDays = parseInt(call.args().get("windowDays"), ctx.windowDays());
        final AgentContext toolCtx = new AgentContext(ctx.storeId(), ctx.query(), windowDays);

        final var toolSample = metrics.toolStart(); // ✅ 无参，修复你现在的编译错误

        return Flux.concat(
                Flux.just(AgentEvent.status("tool_running:" + toolName)),
                toolRegistry.get(toolName).execute(toolCtx)
                        .timeout(timeoutProps.getTool())
                        .doOnNext(collected::add)
                        .map(this::toToolEvent)
                        .flux()
                        .doOnComplete(() -> metrics.toolEnd(toolSample, toolName, "ok"))
                        .onErrorResume(TimeoutException.class, ex -> {
                            metrics.timeout("tool");
                            metrics.toolEnd(toolSample, toolName, "timeout");
                            return Flux.just(
                                    AgentEvent.status("tool_timeout:" + toolName),
                                    AgentEvent.tool(toolTimeoutJson(toolName, timeoutProps.getTool()))
                            );
                        })
                        .onErrorResume(ex -> {
                            metrics.toolEnd(toolSample, toolName, "error");
                            return Flux.just(
                                    AgentEvent.status("tool_error:" + toolName),
                                    AgentEvent.tool("{\"tool\":\"" + toolName + "\",\"error\":\"" + escapeJson(ex.getMessage()) + "\"}")
                            );
                        })
        );
    }

    private AgentEvent toToolEvent(ToolResult r) {
        try {
            return AgentEvent.tool(MAPPER.writeValueAsString(r));
        } catch (Exception e) {
            return AgentEvent.tool("{\"toolName\":\"" + r.toolName() + "\",\"summary\":\"" + escapeJson(r.summary()) + "\"}");
        }
    }

    private String toolTimeoutJson(String toolName, Duration timeout) {
        try {
            return MAPPER.writeValueAsString(Map.of(
                    "tool", toolName,
                    "error", "timeout",
                    "timeoutMs", timeout.toMillis()
            ));
        } catch (Exception e) {
            return "{\"tool\":\"" + escapeJson(toolName) + "\",\"error\":\"timeout\",\"timeoutMs\":" + timeout.toMillis() + "}";
        }
    }

    // ------------------------- summarizer (LLM) -------------------------

    private Flux<AgentEvent> buildSummarizing(AgentContext ctx, List<ToolResult> collected) {
        final var llmSample = metrics.llmStart();

        Flux<AgentEvent> base =
                Mono.fromCallable(() -> buildSummaryPrompt(ctx, collected))
                        .flatMapMany(prompt ->
                                openAIClient.stream(prompt)
                                        .flatMap(line -> {
                                            var err = OpenAISseParser.extractError(line);
                                            if (err.isPresent()) {
                                                // 这里仍然吐 result（保持你现有行为），但不影响最终结构化 result
                                                return Flux.just(
                                                        AgentEvent.status("llm_error"),
                                                        AgentEvent.result(err.get())
                                                );
                                            }
                                            return Mono.justOrEmpty(OpenAISseParser.extractDelta(line))
                                                    .map(AgentEvent::delta)
                                                    .flux();
                                        })
                        )
                        // 合并碎片化 delta：每 50ms 或累计 200 个事件合并一次
                        .bufferTimeout(200, Duration.ofMillis(50))
                        .flatMap(list -> {
                            if (list.isEmpty()) return Flux.empty();

                            boolean hasNonDelta = list.stream().anyMatch(e -> !"delta".equals(e.type()));
                            if (hasNonDelta) return Flux.fromIterable(list);

                            StringBuilder sb = new StringBuilder();
                            for (AgentEvent e : list) sb.append(e.payload());
                            return Flux.just(AgentEvent.delta(sb.toString()));
                        });

        // idle timeout：长时间没有任何事件（含 delta）则判定卡死
        Flux<AgentEvent> withIdle =
                base.timeout(timeoutProps.getLlmIdle())
                        .onErrorResume(TimeoutException.class, ex -> {
                            metrics.timeout("llm_idle");
                            metrics.llmEnd(llmSample, "idle_timeout");
                            return Flux.just(AgentEvent.status(llmTimeoutJson("llm_idle_timeout", timeoutProps.getLlmIdle())));
                        });

        // max timeout：总时长到顶
        return Flux.firstWithSignal(
                        withIdle,
                        Mono.delay(timeoutProps.getLlmMax())
                                .flatMapMany(x -> Flux.error(new TimeoutException("llm_max_timeout")))
                )
                .doOnComplete(() -> {
                    // 注意：如果走到了 idle_timeout/max_timeout 分支，这里不会代表 ok
                    // 这里保守：只有 base 正常完成才算 ok，在 onErrorResume 分支已经结束计时
                    metrics.llmEnd(llmSample, "ok");
                })
                .onErrorResume(TimeoutException.class, ex -> {
                    metrics.timeout("llm_max");
                    metrics.llmEnd(llmSample, "max_timeout");
                    return Flux.just(AgentEvent.status(llmTimeoutJson("llm_max_timeout", timeoutProps.getLlmMax())));
                })
                .onErrorResume(ex -> {
                    // 其他异常（如 downstream cancel 引发的异常等）
                    metrics.llmEnd(llmSample, "error");
                    return Flux.just(AgentEvent.status("{\"type\":\"llm_error\"}"));
                });
    }

    private String llmTimeoutJson(String type, Duration timeout) {
        try {
            return MAPPER.writeValueAsString(Map.of(
                    "type", type,
                    "timeoutMs", timeout.toMillis()
            ));
        } catch (Exception e) {
            return "{\"type\":\"" + escapeJson(type) + "\",\"timeoutMs\":" + timeout.toMillis() + "}";
        }
    }

    // ------------------------- result -------------------------

    private Flux<AgentEvent> buildResultFlow(AgentContext ctx, List<ToolResult> collected, String resultId) {
        // result generator 的 timeout 也打点（可选）
        return resultGenerator.generate(ctx, collected)
                .timeout(RESULT_TIMEOUT)
                .onErrorResume(TimeoutException.class, ex -> {
                    metrics.timeout("result");
                    return Mono.just(ResultFallBackBuilder.build(ctx, collected));
                })
                .onErrorResume(ex -> Mono.just(ResultFallBackBuilder.build(ctx, collected)))
                .flatMapMany(r -> {
                    String savedPath = resultStore.save(resultId, r);

                    String json;
                    try {
                        json = MAPPER.writeValueAsString(r);
                    } catch (Exception e) {
                        json = "{\"error\":\"result_serialize_failed\"}";
                    }

                    String meta = "{\"downloadUrl\":\"/agent/results/" + resultId
                            + "\",\"savedPath\":\"" + escapeJson(savedPath) + "\"}";

                    return Flux.just(
                            AgentEvent.result(json),
                            // 你 Controller 兼容 result_meta / resultMeta，这里建议固定 result_meta
                            AgentEvent.resultMeta(meta)
                    );
                });
    }

    private Flux<AgentEvent> runTimeoutFallback(AgentContext ctx, List<ToolResult> collected, String resultId, Duration runTimeout) {
        var fallback = ResultFallBackBuilder.build(ctx, collected);
        String savedPath = resultStore.save(resultId, fallback);

        String json;
        try {
            json = MAPPER.writeValueAsString(fallback);
        } catch (Exception e) {
            json = "{\"error\":\"fallback_serialize_failed\"}";
        }

        String meta = "{\"downloadUrl\":\"/agent/results/" + resultId
                + "\",\"savedPath\":\"" + escapeJson(savedPath)
                + "\",\"timeoutMs\":" + runTimeout.toMillis() + "}";

        return Flux.just(
                AgentEvent.status("run_timeout"),
                AgentEvent.result(json),
                AgentEvent.resultMeta(meta),
                AgentEvent.status("done")
        );
    }

    // ------------------------- misc -------------------------

    private String buildSummaryPrompt(AgentContext ctx, List<ToolResult> results) throws Exception {
        String toolsJson = MAPPER.writeValueAsString(results);

        return """
                你是电商运营分析助手，请基于工具结果给出结论。
                                
                输出要求：
                1) 指标摘要（3条以内）
                2) 主要原因（按影响排序，3条）
                3) 可执行建议（3条，尽量具体）
                                
                输入：
                storeId=%s
                query=%s
                windowDays=%d
                                
                工具结果（JSON）：
                %s
                """.formatted(ctx.storeId(), ctx.query(), ctx.windowDays(), toolsJson);
    }

    private static int parseInt(Object x, int fallback) {
        try {
            if (x == null) return fallback;
            if (x instanceof Number n) return n.intValue();
            return Integer.parseInt(String.valueOf(x));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private Flux<AgentEvent> stampTrace(Flux<AgentEvent> upstream) {
        return Flux.deferContextual(ctx -> {
            String rid = getRequired(ctx, TraceKeys.RESULT_ID);
            String tid = getRequired(ctx, TraceKeys.TRACE_ID);
            return upstream.map(ev -> ev.withTrace(rid, tid));
        });
    }

    private static String getRequired(ContextView ctx, String key) {
        Object v = ctx.getOrDefault(key, null);
        if (v == null) {
            throw new IllegalStateException("Missing reactor context key: " + key);
        }
        return String.valueOf(v);
    }
}
