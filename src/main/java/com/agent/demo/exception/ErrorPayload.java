package com.agent.demo.exception;

import java.util.Objects;

public record ErrorPayload(ErrorCode code,
                           String message,
                           String stage) {
    public ErrorPayload {
        Objects.requireNonNull(code, "code must not be null");
        // message/stage 允许为空，但建议尽量传
    }

    public static ErrorPayload of(ErrorCode code, String message, String stage) {
        return new ErrorPayload(code, safeMsg(message), stage);
    }

    public static ErrorPayload from(Throwable ex, String stage) {
        // 避免把堆栈/敏感信息直接暴露到外部
        String msg = safeMsg(ex == null ? null : ex.getMessage());
        return new ErrorPayload(ErrorCode.INTERNAL_ERROR, msg, stage);
    }

    public static ErrorPayload timeout(String stage) {
        return new ErrorPayload(ErrorCode.TIMEOUT, "operation timeout", stage);
    }

    public static ErrorPayload rateLimited(String stage) {
        return new ErrorPayload(ErrorCode.RATE_LIMITED, "rate limited", stage);
    }

    private static String safeMsg(String raw) {
        if (raw == null || raw.isBlank()) return "unexpected error";
        // 控制长度，避免把大段信息塞进 SSE
        return raw.length() > 300 ? raw.substring(0, 300) : raw;
    }
}
