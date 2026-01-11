package com.agent.demo.trace;

import org.reactivestreams.Subscription;
import org.slf4j.MDC;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.util.context.ContextView;

import java.util.HashMap;
import java.util.Map;

/**
 * hook 生效后，只要链路里 contextWrite 写过 TraceKeys.*，日志 MDC 就会自动带上
 */
public final class ReactorMdcHook {

    private ReactorMdcHook() {}

    private static final String HOOK_KEY = "agent-mdc";

    public static void install() {
        Hooks.onEachOperator(HOOK_KEY, Operators.lift((sc, sub) -> new MdcSubscriber<>(sub)));
    }

    public static void reset() {
        Hooks.resetOnEachOperator(HOOK_KEY);
    }

    static final class MdcSubscriber<T> implements CoreSubscriber<T> {

        private final CoreSubscriber<? super T> actual;

        MdcSubscriber(CoreSubscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            actual.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            withMdc(actual.currentContext(), () -> actual.onNext(t));
        }

        @Override
        public void onError(Throwable t) {
            withMdc(actual.currentContext(), () -> actual.onError(t));
        }

        @Override
        public void onComplete() {
            withMdc(actual.currentContext(), actual::onComplete);
        }

        @Override
        public reactor.util.context.Context currentContext() {
            return actual.currentContext();
        }

        private static void withMdc(ContextView ctx, Runnable r) {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            try {
                Map<String, String> m = new HashMap<>();
                putIfPresent(m, ctx, TraceKeys.TRACE_ID, "traceId");
                putIfPresent(m, ctx, TraceKeys.RESULT_ID, "resultId");
                putIfPresent(m, ctx, TraceKeys.STORE_ID, "storeId");
                putIfPresent(m, ctx, TraceKeys.WINDOW_DAYS, "windowDays");
                if (!m.isEmpty()) MDC.setContextMap(m);
                r.run();
            } finally {
                if (previous == null) MDC.clear();
                else MDC.setContextMap(previous);
            }
        }

        private static void putIfPresent(Map<String, String> m, ContextView ctx, String key, String mdcKey) {
            Object v = ctx.getOrDefault(key, null);
            if (v != null) m.put(mdcKey, String.valueOf(v));
        }
    }
}
