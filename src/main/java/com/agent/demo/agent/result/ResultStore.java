package com.agent.demo.agent.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * ResultStore: 用于落盘 / 读取
 */
@Service
public class ResultStore {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 放在项目根目录下的 data/results（不污染 resources）
    private final Path baseDir = Paths.get("data", "results");

    public ResultStore() {
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create result dir: " + baseDir.toAbsolutePath(), e);
        }
    }

    public String save(String resultId, AgentResult result) {
        Path file = filePath(resultId);
        try {
            // 带一点元信息也行：这里直接存 AgentResult 本身即可
            String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result);
            Files.writeString(file, json, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            return file.toAbsolutePath().toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to save result: " + resultId, e);
        }
    }

    public String loadRaw(String resultId) {
        Path file = filePath(resultId);
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("result_not_found: " + resultId);
        }
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read result: " + resultId, e);
        }
    }

    public Path filePath(String resultId) {
        // 防止目录穿越，只允许简单 id
        if (resultId == null || !resultId.matches("[a-zA-Z0-9_-]{6,80}")) {
            throw new IllegalArgumentException("invalid_result_id");
        }
        return baseDir.resolve(resultId + ".json");
    }
}
