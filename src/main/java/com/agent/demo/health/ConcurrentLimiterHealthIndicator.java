package com.agent.demo.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import reactor.core.publisher.Mono;

/**
 * ConcurrencyLimiter 状态检查: 检查配置值 + bean 存在
 */
public class ConcurrentLimiterHealthIndicator implements ReactiveHealthIndicator {
    private final Object limiterBean; // 只要存在即可
    private final int globalMax;
    private final int perStoreMax;
    private final int maxStores;

    public ConcurrentLimiterHealthIndicator(Object limiterBean, int globalMax, int perStoreMax, int maxStores) {
        this.limiterBean = limiterBean;
        this.globalMax = globalMax;
        this.perStoreMax = perStoreMax;
        this.maxStores = maxStores;
    }

    @Override
    public Mono<Health> health() {
        return Mono.fromSupplier(() -> {
            if (limiterBean == null) {
                return Health.down().withDetail("limiter", "MISSING_BEAN").build();
            }
            if (globalMax <= 0) {
                return Health.down().withDetail("agent.limits.globalMaxConcurrent", "INVALID").build();
            }
            if (perStoreMax <= 0) {
                return Health.down().withDetail("agent.limits.perStoreMaxConcurrent", "INVALID").build();
            }
            if (maxStores <= 0) {
                return Health.down().withDetail("agent.limits.maxStores", "INVALID").build();
            }
            return Health.up()
                    .withDetail("globalMaxConcurrent", globalMax)
                    .withDetail("perStoreMaxConcurrent", perStoreMax)
                    .withDetail("maxStores", maxStores)
                    .build();
        });
    }
}