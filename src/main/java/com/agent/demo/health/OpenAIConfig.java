package com.agent.demo.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenAIConfig {

    @Bean(name = "openaiConfigHealth")
    public ReactiveHealthIndicator openAiConfigHealth(
            @Value("${openai.base-url:}") String baseUrl,
            @Value("${openai.api-key:}") String apiKey,
            @Value("${openai.model:}") String model) {
        return new OpenAIConfigHealthIndicator(baseUrl, apiKey, model);
    }
}