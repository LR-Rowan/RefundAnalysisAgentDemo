package com.agent.demo.agent;

import com.agent.demo.agent.plan.Plan;
import com.agent.demo.agent.plan.Planner;
import com.agent.demo.agent.plan.ToolCall;
import com.agent.demo.agent.result.ResultFallBackBuilder;
import com.agent.demo.agent.result.ResultGenerator;
import com.agent.demo.agent.result.ResultStore;
import com.agent.demo.llm.OpenAIClient;
import com.agent.demo.llm.OpenAISseParser;
import com.agent.demo.tools.ToolRegistry;
import com.agent.demo.tools.ToolResult;
import com.agent.demo.trace.IdGenerators;
import com.agent.demo.trace.TraceKeys;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Agent - 编排器
 */
@Service
public class AgentOrchestrator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Planner planner;
    private final ToolRegistry toolRegistry;
    private final OpenAIClient openAIClient;
    private final ResultGenerator resultGenerator;
    private final ResultStore resultStore;

    public AgentOrchestrator(Planner planner, ToolRegistry toolRegistry, OpenAIClient openAIClient, ResultGenerator resultGenerator, ResultStore resultStore) {
        this.planner = planner;
        this.toolRegistry = toolRegistry;
        this.openAIClient = openAIClient;
        this.resultGenerator = resultGenerator;
        this.resultStore = resultStore;
    }

    public Flux<AgentEvent> run(AgentContext ctx, String incomingTraceId) {
        final String resultId = IdGenerators.newResultId();
        // traceId: 优先用上游传入（HTTP Header），否则生成
        final String traceId =(Objects.nonNull(incomingTraceId) && !incomingTraceId.isBlank())
                ? incomingTraceId : IdGenerators.newTraceId();

        Flux<AgentEvent> start = Flux.just(AgentEvent.status("planning"));

        Mono<Plan> planMono = planner.plan(ctx)
                .onErrorResume(ex -> {
                    // fallback：planner 解析失败时用默认工具集
                    Plan fallback = new Plan(List.of(
                            new ToolCall("refund_rate", java.util.Map.of("windowDays", ctx.windowDays())),
                            new ToolCall("logistics_delay", java.util.Map.of("windowDays", ctx.windowDays()))
                    ));
                    return Mono.just(fallback);
                });

        // 输出 planner 选了哪些 tools
        Flux<AgentEvent> planEvent = planMono
                .map(plan -> {
                    try {
                        String planJson = MAPPER.writeValueAsString(plan);
                        return AgentEvent.tool("{\"toolName\":\"planner\",\"summary\":\"plan_selected\",\"metrics\":" + planJson + "}");
                    } catch (Exception e) {
                        return AgentEvent.tool("{\"toolName\":\"planner\",\"summary\":\"plan_selected\"}");
                    }
                })
                .flux();

        // 收集工具结果，供 summarizing 使用
        List<ToolResult> collected = new ArrayList<>();

        Flux<AgentEvent> toolsFlow = planMono.flatMapMany(plan ->
                Flux.fromIterable(plan.tools())
                        .concatMap(call -> {
                            String toolName = call.name();
                            // 参数目前只用 windowDays（你后面可扩展更多 args）
                            int windowDays = parseInt(call.args().get("windowDays"), ctx.windowDays());
                            AgentContext toolCtx = new AgentContext(ctx.storeId(), ctx.query(), windowDays);

                            return Flux.concat(
                                    Flux.just(AgentEvent.status("tool_running:" + toolName)),
                                    toolRegistry.get(toolName).execute(toolCtx)
                                            .doOnNext(collected::add)
                                            .map(this::toToolEvent)
                                            .flux()
                                            .onErrorResume(ex -> Flux.just(
                                                    AgentEvent.status("tool_error:" + toolName),
                                                    AgentEvent.tool("{\"tool\":\"" + toolName + "\",\"error\":\"" + escapeJson(ex.getMessage()) + "\"}")
                                            ))
                            );
                        })
        );

        Flux<AgentEvent> summarizingStart = Flux.just(AgentEvent.status("summarizing"));

        Flux<AgentEvent> summarizingFlow =
                Mono.fromCallable(() -> buildSummaryPrompt(ctx, collected))
                        .flatMapMany(prompt ->
                                openAIClient.stream(prompt)
                                        .flatMap(line -> {
                                            // 解析 error
                                            var err = OpenAISseParser.extractError(line);
                                            if (err.isPresent()) {
                                                return Flux.just(
                                                        AgentEvent.status("llm_error"),
                                                        AgentEvent.result(err.get())
                                                );
                                            }
                                            // 解析 delta
                                            return Mono.justOrEmpty(OpenAISseParser.extractDelta(line))
                                                    .map(AgentEvent::delta)
                                                    .flux();
                                        })
                        )
                        // 合并碎片化 delta：每 50ms 或累计 200 个事件合并一次
                        .bufferTimeout(200, java.time.Duration.ofMillis(50))
                        .flatMap(list -> {
                            if (list.isEmpty()) return Flux.empty();

                            // 非 delta（如 llm_error/result）原样透传
                            boolean hasNonDelta = list.stream().anyMatch(e -> !"delta".equals(e.type()));
                            if (hasNonDelta) return Flux.fromIterable(list);

                            // 合并 delta payload
                            StringBuilder sb = new StringBuilder();
                            for (AgentEvent e : list) sb.append(e.payload());
                            return Flux.just(AgentEvent.delta(sb.toString()));
                        });

        Flux<AgentEvent> resultStart = Flux.just(AgentEvent.status("result_generating"));

        Flux<AgentEvent> resultFlow =
                resultGenerator.generate(ctx, collected)
                        .timeout(Duration.ofSeconds(60))
                        .onErrorResume(ex -> {
                            // 超时/失败 -> 本地兜底结构化结果
                            return Mono.just(ResultFallBackBuilder.build(ctx, collected));
                        })
                        .flatMapMany(r -> {
                            // 1) 落盘
                            String savedPath = resultStore.save(resultId, r);

                            // 2) 输出 result + meta
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
                                    AgentEvent.resultMeta(meta)
                            );
                        });

        Flux<AgentEvent> end = Flux.just(AgentEvent.status("done"));

        return Flux.concat(start, planEvent, toolsFlow, summarizingStart, summarizingFlow, resultStart, resultFlow, end)
                // cancel 观测点 + 确保取消信号能触发
                .doOnCancel(() -> System.out.println("[CANCEL] client disconnected"))
                .doFinally(signal -> System.out.println("[FINALLY] signal=" + signal))
                // 统一 stamp：保证每个 AgentEvent 都带 rid/tid（包括 fallback/timeout/工具异常产生的事件）
                //
                // stampTrace 在下游会读不到 contextWrite 写入的 key，产生报错: "Missing reactor context key: resultId"
                .transform(this::stampTrace)
                // 全链路挂上 traceId/resultId（Planner/Tools/Summarizer/Result/End 全覆盖）
                .contextWrite(c -> c.put(TraceKeys.RESULT_ID, resultId).put(TraceKeys.TRACE_ID, traceId));
    }

    private AgentEvent toToolEvent(ToolResult r) {
        try {
            return AgentEvent.tool(MAPPER.writeValueAsString(r));
        } catch (Exception e) {
            return AgentEvent.tool("{\"toolName\":\"" + r.toolName() + "\",\"summary\":\"" + escapeJson(r.summary()) + "\"}");
        }
    }

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
            // 生产级：不要 silent fail，否则 trace 丢了难排查
            throw new IllegalStateException("Missing reactor context key: " + key);
        }
        return String.valueOf(v);
    }
}
