package com.agent.demo.infra;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class LimiterCleanupJob {

    private final ConcurrencyLimiter limiter;

    public LimiterCleanupJob(ConcurrencyLimiter limiter) {
        this.limiter = limiter;
    }

    /**
     * 清理空闲 store limiter，避免 perStore map 无限增长
     * 每 5 分钟执行一次（生产常见）
     */
    @Scheduled(fixedDelayString = "PT5M", initialDelayString = "PT1M")
    public void cleanupLimiter() {
        int removed = limiter.cleanupIdleStores();
        if (removed > 0) {
            System.out.println("[LIMIT] cleanup removed=" + removed);
        }
    }
}