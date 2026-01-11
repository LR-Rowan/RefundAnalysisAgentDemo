package com.agent.demo.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/**
 * AgentExceptionHandler - 全局异常处理器，专门把 429 错误带 reason 输出
 */
@RestControllerAdvice
public class AgentExceptionHandler {

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<String> handle(ResponseStatusException ex) {
        if (ex.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
            String msg = ex.getReason() == null ? "rate_limited" : ex.getReason();
            // 自定义 429 body（生产可观测）
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"error\":\"too_many_requests\",\"message\":\"" + escape(msg) + "\"}");
        }
        // 其它 status 不动：交给默认格式也行
        return ResponseEntity.status(ex.getStatusCode())
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"error\":\"" + ex.getStatusCode().value() + "\",\"message\":\"" + escape(ex.getReason()) + "\"}");
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}