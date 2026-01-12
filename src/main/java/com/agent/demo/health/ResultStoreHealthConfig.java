package com.agent.demo.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 注册到 Spring 并纳入 readiness group
 */
@Configuration
public class ResultStoreHealthConfig {

    @Bean(name = "resultStoreHealth")
    public ReactiveHealthIndicator resultStoreHealthIndicator(@Value("${agent.result-store.dir}") String resultDir) {
        return new ResultStoreHealthIndicator(Path.of(resultDir));
    }
}
