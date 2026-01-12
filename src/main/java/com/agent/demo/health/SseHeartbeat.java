package com.agent.demo.health;

import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * 心跳 Flux
 */
public final class SseHeartbeat {
    private SseHeartbeat() {}

    public static Flux<String> comments(Duration interval, String comment) {
        // SSE 注释行：": ping\n\n"
        return Flux.interval(interval)
                .map(tick -> ":" + comment + "\n\n");
    }
}