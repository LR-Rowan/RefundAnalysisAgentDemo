package com.agent.demo.controller;

import com.agent.demo.dto.RunRequest;
import com.agent.demo.llm.OpenAIClient;
import com.agent.demo.llm.OpenAISseParser;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;


/**
 * SSE Controller
 * <p>
 * 这个 Controller 里的方法 不会阻塞线程等结果
 * 而是 立即返回一个 Flux（数据流）
 *
 */
@RestController
@RequestMapping("/agent")
public class AgentController {

    @Autowired
    private OpenAIClient openAIClient;

    /**
     * produces = TEXT_EVENT_STREAM: 告诉Spring返回的是SSE, 浏览器会一条条接收, 避免Flux一次性聚合
     */
    @PostMapping(
            value = "/run",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<String>> run(@Valid @RequestBody RunRequest request) {
        Flux<ServerSentEvent<String>> start = Flux.just(
                sse("status", "run_started"),
                sse("status", "llm_stream_start"));

        String prompt = """
                你是电商运营分析助手。
                用户问题：%s
                请输出：
                1. 问题概述
                2. 可能原因（3条）
                3. 可执行建议（3条）
                """.formatted(request.query());
        Flux<ServerSentEvent<String>> llmStream =
                openAIClient.stream(prompt)
                        .flatMap(line -> {
                            return OpenAISseParser.extractError(line)
                                    .<Flux<ServerSentEvent<String>>>map(err -> Flux.just(
                                            sse("status", "llm_error"),
                                            sse("result", err)
                                    ))
                                    .orElseGet(() -> OpenAISseParser.extractDelta(line)
                                            .<Flux<ServerSentEvent<String>>>map(delta -> Flux.just(sse("delta", delta)))
                                            .orElseGet(Flux::empty));
                        })
                        // 关键：把很多小 delta 合并成更大块（50ms 一次）
                        .bufferTimeout(200, java.time.Duration.ofMillis(50))
                        .flatMap(list -> {
                            if (list.isEmpty()) return Flux.empty();

                            // 只合并 delta，status/result 原样透传
                            boolean hasNonDelta = list.stream().anyMatch(e -> !"delta".equals(e.event()));
                            if (hasNonDelta) return Flux.fromIterable(list);

                            StringBuilder sb = new StringBuilder();
                            for (ServerSentEvent<String> e : list) sb.append(e.data());
                            return Flux.just(sse("delta", sb.toString()));
                        });

        Flux<ServerSentEvent<String>> end = Flux.just(
                sse("status", "llm_stream_end"),
                sse("status", "done")
        );

        return Flux.concat(start, llmStream, end);
    }

    // 单独封装sse()工具方法, 所有SSE构造方式统一
    private ServerSentEvent<String> sse(String event, String data) {
        return ServerSentEvent.<String>builder()
                .event(event)
                .data(data)
                .build();
    }
}
