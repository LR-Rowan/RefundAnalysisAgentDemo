package com.agent.demo.llm;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.transport.ProxyProvider;

import java.time.Duration;
import java.util.Map;

/**
 * WebClient + Streaming
 */
@Component
public class OpenAIClient {
    private final WebClient webClient;
    private final String model;

    public OpenAIClient(
            @Value("${openai.base-url}") String baseUrl,
            @Value("${openai.api-key}") String apiKey,
            @Value("${openai.model}") String model) {
        HttpClient httpClient = HttpClient.create()
                .proxy(spec -> spec
                        .type(ProxyProvider.Proxy.HTTP)
                        .host("127.0.0.1")
                        .port(7890))
                .responseTimeout(Duration.ofSeconds(60));

        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                // 在 Reactor Netty 里显式配置 HTTP Proxy, 强制 OpenAI 请求走 Clash 代理
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .build();
        this.model = model;
    }

    /**
     * 返回 OpenAI 的原始 SSE 行（每一行是 "data: {...}"）
     */
    public Flux<String> stream(String prompt) {
        Map<String, Object> body = Map.of(
                "model", model,
                "input", prompt,
                "stream", true
        );

        return webClient.post()
                .uri("/v1/responses")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)        // 告诉OpenAI要流式
                .bodyValue(body)
                .exchangeToFlux(resp -> {
                    if (resp.statusCode().isError()) {
                        return resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMapMany(msg -> Flux.error(
                                        new RuntimeException("OpenAI HTTP " + resp.statusCode() + " body=" + msg)
                                ));
                    }
                    // 直接流式读取响应体；下游 cancel 会取消订阅并关闭连接
                    return resp.bodyToFlux(String.class);
                })
                // 取消可观测（证明 Ctrl+C 能停掉上游）
                .doOnCancel(() -> System.out.println("[OPENAI] stream cancelled by downstream"))
                .doFinally(sig -> System.out.println("[OPENAI] stream finished signal=" + sig));
    }

    /**
     * 非流式一次性调用, 用于Planner
     *
     * @param prompt String
     * @return Mono<String>
     */
    public Mono<String> callOnce(String prompt) {
        Map<String, Object> body = Map.of(
                "model", model,
                "input", prompt,
                "stream", false
        );

        return webClient.post()
                .uri("/v1/responses")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(s -> s.isError(), resp ->
                        resp.bodyToMono(String.class).flatMap(msg ->
                                Mono.error(new RuntimeException("OpenAI HTTP " + resp.statusCode() + " body=" + msg))
                        ))
                .bodyToMono(String.class);
    }
}
