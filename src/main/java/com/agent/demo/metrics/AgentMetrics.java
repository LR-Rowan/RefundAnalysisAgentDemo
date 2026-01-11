package com.agent.demo.metrics;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 统一埋点入口，避免散落打点
 *
 * 生产级约束：
 * 1) 不要在 hot path 里反复 builder/register（改为缓存 meter）
 * 2) Timer 建议使用 histogram/percentiles，避免 MAX=0 / 无分布
 * 3) meter 命名统一：Timer 默认以 seconds 暴露（Prometheus/Actuator），避免 _ms 误导
 */
@Component
public class AgentMetrics {

    private final MeterRegistry registry;

    // ------------------ caches (avoid repeated builder/register) ------------------
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();

    // ------------------ inflight gauge ------------------
    private final AtomicInteger inflight = new AtomicInteger(0);

    // 统一的延迟分布配置（按需调整）
    private static final double[] DEFAULT_PERCENTILES = new double[]{0.5, 0.9, 0.95, 0.99};

    // Service Level Objectives（可选）：让 histogram bucket 更有意义
    // 这里给一组“适合你当前 demo”的默认值：100ms/500ms/1s/3s/10s/30s/60s
    private static final Duration[] DEFAULT_SLOS = new Duration[]{
            Duration.ofMillis(100),
            Duration.ofMillis(500),
            Duration.ofSeconds(1),
            Duration.ofSeconds(3),
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            Duration.ofSeconds(60)
    };

    public AgentMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder("agent_inflight", inflight, AtomicInteger::get)
                .description("Number of in-flight agent runs")
                .register(registry);
    }

    // ---------- inflight ----------
    public void inflightInc() { inflight.incrementAndGet(); }
    public void inflightDec() { inflight.decrementAndGet(); }

    // ---------- run ----------
    public Timer.Sample runStart() {
        return Timer.start(registry);
    }

    public void runEnd(Timer.Sample sample, String outcome) {
        String o = safe(outcome, "unknown");
        Timer t = timer("agent_run_latency", "Agent run latency", // 注意：不要叫 *_ms，Timer 暴露默认是 seconds
                "outcome", o);

        sample.stop(t);

        counter("agent_run_total", "Agent run total",
                "outcome", o).increment();
    }

    // ---------- rate limit ----------
    public void rateLimited(String reason) {
        String r = safe(reason, "unknown");
        counter("agent_rate_limited_total", "Rate limited requests",
                "reason", r).increment();
    }

    // ---------- tool ----------
    public Timer.Sample toolStart() {
        return Timer.start(registry);
    }

    public void toolEnd(Timer.Sample sample, String toolName, String outcome) {
        String tool = safe(toolName, "unknown");
        String o = safe(outcome, "unknown");

        Timer t = timer("agent_tool_latency", "Tool execution latency",
                "tool", tool,
                "outcome", o);
        sample.stop(t);

        counter("agent_tool_total", "Tool execution total",
                "tool", tool,
                "outcome", o).increment();
    }

    // ---------- llm ----------
    public Timer.Sample llmStart() {
        return Timer.start(registry);
    }

    public void llmEnd(Timer.Sample sample, String outcome) {
        String o = safe(outcome, "unknown");

        Timer t = timer("agent_llm_stream_duration", "LLM summarizer stream duration",
                "outcome", o);
        sample.stop(t);

        counter("agent_llm_total", "LLM summarizer total",
                "outcome", o).increment();
    }

    // ---------- timeouts (optional but useful) ----------
    public void timeout(String scope) {
        String s = safe(scope, "unknown");
        counter("agent_timeout_total", "Timeout total by scope",
                "scope", s).increment();
    }

    /**
     * 直接记录一个耗时（如果你不想用 Sample）
     */
    public void recordMs(String name, long ms, String... tags) {
        Timer t = timer(name, name, tags);
        t.record(ms, TimeUnit.MILLISECONDS);
    }

    // ------------------ internal helpers ------------------

    private Counter counter(String name, String desc, String... tags) {
        final String key = key(name, tags);
        return counters.computeIfAbsent(key, k ->
                Counter.builder(name)
                        .description(desc)
                        .tags(tags)
                        .register(registry)
        );
    }

    private Timer timer(String name, String desc, String... tags) {
        final String key = key(name, tags);
        return timers.computeIfAbsent(key, k -> {
            Timer.Builder b = Timer.builder(name)
                    .description(desc)
                    .tags(tags)
                    // 关键：没有 histogram/percentiles 时，很多情况下 MAX/分位数不好用
                    .publishPercentileHistogram()
                    .publishPercentiles(DEFAULT_PERCENTILES)
                    // 可选：设置 SLO buckets（更接近生产的可观测需求）
                    .serviceLevelObjectives(DEFAULT_SLOS);

            return b.register(registry);
        });
    }

    private static String key(String name, String... tags) {
        // tags 是 k1,v1,k2,v2...，保证 key 稳定
        return name + "|" + Arrays.toString(tags);
    }

    private static String safe(String s, String fallback) {
        if (s == null) return fallback;
        String t = s.trim();
        return t.isEmpty() ? fallback : t;
    }
}
