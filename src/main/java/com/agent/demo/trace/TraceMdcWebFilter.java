package com.agent.demo.trace;

import org.slf4j.MDC;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * WebFlux 入口：提取/生成 traceId，写入 Reactor Context + MDC。
 */
@Component
public class TraceMdcWebFilter implements WebFilter {
    private static final String HEADER_TRACE_ID = "X-Trace-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest req = exchange.getRequest();
        String incoming = req.getHeaders().getFirst(HEADER_TRACE_ID);
        String traceId = (Objects.nonNull(incoming) && !incoming.isBlank())
                ? incoming
                : IdGenerators.newTraceId();
        // 先把 traceId 放进 MDC（便于入口日志）
        MDC.put("traceId", traceId);

        return chain.filter(exchange)
                // 把 traceId 写入 Reactor Context，后续 Orchestrator/Tools 都能拿到
                .contextWrite(ctx -> ctx.put(TraceKeys.TRACE_ID, traceId))
                // 每个信号把 Context 同步到 MDC（resultId 之后也会出现）
                .doOnEach(ReactorMdc.lift())
                .doFinally(sig -> ReactorMdc.clear());
    }
}
