package com.agent.demo.trace;


import org.slf4j.MDC;
import reactor.core.publisher.Signal;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 将 Reactor Context 中的 traceId/resultId 等写入 MDC，便于日志自动带字段
 * 用法: .doOnEach(ReactorMdc.lift())
 */
public class ReactorMdc {
    private ReactorMdc() {}

    public static <T> Consumer<Signal<T>> lift() {
        return signal -> {
            if (!signal.isOnNext() && !signal.isOnError() && !signal.isOnComplete()) {
                return;
            }

            var ctx = signal.getContextView();
            putIfPresent(ctx, TraceKeys.TRACE_ID, "traceId");
            putIfPresent(ctx, TraceKeys.RESULT_ID, "resultId");
            putIfPresent(ctx, TraceKeys.STORE_ID, "storeId");
            putIfPresent(ctx, TraceKeys.WINDOW_DAYS, "windowDays");
        };
    }

    private static void putIfPresent(reactor.util.context.ContextView ctx, String key, String mdcKey) {
        Object v = ctx.getOrDefault(key, null);
        if (Objects.isNull(v)) {
            return;
        }
        MDC.put(mdcKey, String.valueOf(v));
    }

    public static void clear() {
        MDC.remove("traceId");
        MDC.remove("resultId");
        MDC.remove("storeId");
        MDC.remove("windowDays");
    }
}
