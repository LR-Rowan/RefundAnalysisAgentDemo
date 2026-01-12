package com.agent.demo.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(-1000)
public class RequestLogFilter implements WebFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestLogFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest req = exchange.getRequest();
        // 只针对 /agent/run 打印请求头，先不读 body（避免二次消费）
        if (req.getPath().value().equals("/agent/run")) {
            LOGGER.info("req headers contentType={} contentLength={} accept={}",
                    req.getHeaders().getContentType(),
                    req.getHeaders().getContentLength(),
                    req.getHeaders().getFirst("Accept"));
        }
        return chain.filter(exchange);
    }
}
