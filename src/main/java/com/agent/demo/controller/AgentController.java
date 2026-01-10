package com.agent.demo.controller;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.agent.AgentOrchestrator;
import com.agent.demo.dto.RunRequest;
import com.agent.demo.llm.OpenAIClient;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

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

    @Autowired
    private AgentOrchestrator agentOrchestrator;

    /**
     * produces = TEXT_EVENT_STREAM: 告诉Spring返回的是SSE, 浏览器会一条条接收, 避免Flux一次性聚合
     */
    @PostMapping(
            value = "/run",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<String>> run(@Valid @RequestBody RunRequest request) {

        AgentContext ctx = new AgentContext(request.storeId(), request.query(), 7);

        return agentOrchestrator.run(ctx)
                .map(evt -> ServerSentEvent.<String>builder()
                        .event(evt.type())
                        .data(evt.payload())
                        .build());
    }
}
