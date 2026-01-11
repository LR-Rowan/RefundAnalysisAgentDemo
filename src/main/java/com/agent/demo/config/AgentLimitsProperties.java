package com.agent.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "agent.limits")
public class AgentLimitsProperties {
    /**
     * 全局最大并发 run 数
     */
    private int globalMaxConcurrent = 10;

    /**
     * 单个 storeId 最大并发 run 数
     */
    private int perStoreMaxConcurrent = 2;

    /**
     * store limiter 空闲清理 TTL（分钟）
     */
    private int storeIdleTtlMinutes = 30;

    /**
     * store limiter 最大条目数（防止 map 无限增长）
     */
    private int maxStores = 10000;

    public int getGlobalMaxConcurrent() {
        return globalMaxConcurrent;
    }

    public void setGlobalMaxConcurrent(int globalMaxConcurrent) {
        this.globalMaxConcurrent = globalMaxConcurrent;
    }

    public int getPerStoreMaxConcurrent() {
        return perStoreMaxConcurrent;
    }

    public void setPerStoreMaxConcurrent(int perStoreMaxConcurrent) {
        this.perStoreMaxConcurrent = perStoreMaxConcurrent;
    }

    public int getStoreIdleTtlMinutes() {
        return storeIdleTtlMinutes;
    }

    public void setStoreIdleTtlMinutes(int storeIdleTtlMinutes) {
        this.storeIdleTtlMinutes = storeIdleTtlMinutes;
    }

    public int getMaxStores() {
        return maxStores;
    }

    public void setMaxStores(int maxStores) {
        this.maxStores = maxStores;
    }
}
