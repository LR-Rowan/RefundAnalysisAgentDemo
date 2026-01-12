package com.agent.demo.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 检查结果目录可写
 */
public class ResultStoreHealthIndicator implements ReactiveHealthIndicator {
    private final Path resultDir;

    public ResultStoreHealthIndicator(Path resultDir) {
        this.resultDir = resultDir;
    }

    @Override
    public Mono<Health> getHealth(boolean includeDetails) {
        return ReactiveHealthIndicator.super.getHealth(includeDetails);
    }

    @Override
    public Mono<Health> health() {
        return Mono.fromCallable(() -> {
            if (!Files.exists(resultDir)) {
                return Health.down().withDetail("resultDir", "NOT_EXISTS").build();
            }
            if (!Files.isDirectory(resultDir)) {
                return Health.down().withDetail("resultDir", "NOT_DIRECTORY").build();
            }
            if (!Files.isWritable(resultDir)) {
                return Health.down().withDetail("resultDir", "NOT_WRITABLE").build();
            }
            return Health.up().withDetail("resultDir", "OK").build();
        }).onErrorReturn(Health.down().withDetail("resultDir", "ERROR").build());
    }
}
