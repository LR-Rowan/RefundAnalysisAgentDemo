package com.agent.demo.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.codec.DecodingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;

/**
 * AgentExceptionHandler - 全局异常处理器，专门把 429 错误带 reason 输出
 */
@RestControllerAdvice
public class AgentExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentExceptionHandler.class);

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

    // 把“Failed to read HTTP message”的真正原因打印出来
    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<String> handleInput(ServerWebInputException ex) {
        LOGGER.warn("server_web_input_exception msg={} cause={}",
                ex.getMessage(),
                ex.getCause() == null ? "null" : ex.getCause().toString(),
                ex);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("{\"error\":\"400\",\"message\":\"" + escape(ex.getMessage()) + "\"}");
    }

    @ExceptionHandler(DecodingException.class)
    public ResponseEntity<String> handleDecoding(DecodingException ex) {
        LOGGER.warn("decoding_exception msg={} cause={}",
                ex.getMessage(),
                ex.getCause() == null ? "null" : ex.getCause().toString(),
                ex);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("{\"error\":\"400\",\"message\":\"JSON decoding error\"}");
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}