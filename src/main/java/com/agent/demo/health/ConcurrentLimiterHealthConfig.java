package com.agent.demo.health;

import com.agent.demo.infra.ConcurrencyLimiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConcurrentLimiterHealthConfig {

    @Bean(name = "concurrencyLimiterHealth")
    public ReactiveHealthIndicator concurrencyLimiterHealth(
            ConcurrencyLimiter limiter,
            @Value("${agent.limits.globalMaxConcurrent}") int globalMax,
            @Value("${agent.limits.perStoreMaxConcurrent}") int perStoreMax,
            @Value("${agent.limits.maxStores}") int maxStores
    ) {
        return new ConcurrentLimiterHealthIndicator(limiter, globalMax, perStoreMax, maxStores);
    }
}
