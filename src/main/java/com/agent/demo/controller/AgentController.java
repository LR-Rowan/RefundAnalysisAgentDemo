package com.agent.demo.controller;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.agent.AgentOrchestrator;
import com.agent.demo.agent.result.ResultStore;
import com.agent.demo.dto.RunRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

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
    private static final int WINDOW_DAYS = 7;
    private static final ObjectMapper mapper = new ObjectMapper();

    private record SseEnvelope(String resultId, String traceId, Object data) {}

    @Autowired
    private AgentOrchestrator agentOrchestrator;

    @Autowired
    private ResultStore resultStore;

    /**
     * produces = TEXT_EVENT_STREAM: 告诉Spring返回的是SSE, 浏览器会一条条接收, 避免Flux一次性聚合
     */
    @PostMapping(
            value = "/run",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<String>> run(
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestParam(value = "legacy", required = false, defaultValue = "false") boolean legacy,
            @Valid @RequestBody RunRequest request) {
        AgentContext ctx = new AgentContext(request.storeId(), request.query(), WINDOW_DAYS);

        return agentOrchestrator.run(ctx, traceId)
                .map(evt -> {
                    String data;

                    if (legacy) {
                        // 旧协议：不包 envelope，保持原样输出
                        data = evt.payload();
                    } else {
                        Object payloadObj = tryParseJson(evt.payload());

                        SseEnvelope env = new SseEnvelope(
                                evt.resultId(),
                                evt.traceId(),
                                payloadObj
                        );

                        try {
                            data = mapper.writeValueAsString(env);
                        } catch (Exception e) {
                            // 生产级容错：序列化失败不能打断 SSE stream
                            data = "{\"resultId\":\"" + safe(evt.resultId()) +
                                    "\",\"traceId\":\"" + safe(evt.traceId()) +
                                    "\",\"data\":\"json_serialize_failed\"}";
                        }
                    }

                    return ServerSentEvent.<String>builder()
                            .event(evt.type())
                            .data(data)
                            .build();
                });

    }

    /**
     * 下载接口, 浏览器/curl 直接能拿到 JSON
     * <p>
     *
     * @param resultId String
     * @return Mono<ResponseEntity<String>>
     */
    @GetMapping(value = "/results/{resultId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> downloadResult(@PathVariable("resultId") String resultId) {
        return Mono.fromCallable(() -> resultStore.loadRaw(resultId))
                .map(body -> ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(body)
                )
                .onErrorResume(ex -> Mono.just(
                        ResponseEntity.status(404)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body("{\"error\":\"not_found\",\"message\":\"" + ex.getMessage().replace("\"", "\\\"") + "\"}")
                ));
    }

    // 如果 evt.payload() 是 JSON → 解析成 JSON 对象，若不是则保持为 String
    //
    // 让 data变为 JSON对象，而不是字符串，可让前端少一次 JSON.parse
    private Object tryParseJson(String payload) {
        if (Objects.isNull(payload) || payload.isBlank()) {
            return payload;
        }

        String s = payload.trim();
        if (!(s.startsWith("{") || s.startsWith("["))) {
            // 明显不是 JSON
            return payload;
        }

        try {
            return mapper.readTree(s); // 返回 JsonNode
        } catch (Exception e) {
            // 不是合法 JSON，当普通字符串
            return payload;
        }
    }

    // 避免 null / 引号炸裂
    private static String safe(String s) {
        if (Objects.isNull(s)) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
