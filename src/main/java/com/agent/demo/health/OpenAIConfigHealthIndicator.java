package com.agent.demo.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

/**
 * OpenAI 配置完整性检查
 */
public class OpenAIConfigHealthIndicator implements ReactiveHealthIndicator {

    private final String baseUrl;
    private final String apiKey;
    private final String model;

    public OpenAIConfigHealthIndicator(String baseUrl, String apiKey, String model) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.model = model;
    }

    @Override
    public Mono<Health> health() {
        return Mono.fromSupplier(() -> {
            // base-url / api-key / model 必须有
            if (!StringUtils.hasText(baseUrl)) {
                return Health.down().withDetail("openai.base-url", "MISSING").build();
            }
            if (!StringUtils.hasText(apiKey)) {
                return Health.down().withDetail("openai.api-key", "MISSING").build();
            }
            if (!StringUtils.hasText(model)) {
                return Health.down().withDetail("openai.model", "MISSING").build();
            }

            // 避免把 key 明文打出去
            String masked = mask(apiKey);

            return Health.up()
                    .withDetail("openai.base-url", baseUrl)
                    .withDetail("openai.model", model)
                    .withDetail("openai.api-key", masked)
                    .build();
        });
    }

    private static String mask(String key) {
        if (!StringUtils.hasText(key)) return "";
        int n = key.length();
        if (n <= 6) return "***";
        return key.substring(0, 3) + "***" + key.substring(n - 3);
    }
}