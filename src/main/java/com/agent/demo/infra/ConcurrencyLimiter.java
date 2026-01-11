package com.agent.demo.infra;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * ConcurrencyLimiter - 公平信号量 + TTL 清理 + maxStores 防护
 */
public class ConcurrencyLimiter {
    private final Semaphore global;
    private final ConcurrentHashMap<String, StoreEntry> perStore = new ConcurrentHashMap<>();

    private final int perStoreLimit;
    private final int maxStores;
    private final Duration storeIdleTtl;

    public ConcurrencyLimiter(int globalLimit, int perStoreLimit, int maxStores, Duration storeIdleTtl) {
        // 公平信号量：生产更可预期（防止“饿死”）
        this.global = new Semaphore(globalLimit, true);
        this.perStoreLimit = perStoreLimit;
        this.maxStores = maxStores;
        this.storeIdleTtl = storeIdleTtl;
    }

    public Permit tryAcquire(String storeId) {
        if (storeId == null || storeId.isBlank()) {
            return Permit.denied("invalid_store");
        }

        // 1) global bulkhead
        if (!global.tryAcquire()) {
            return Permit.denied("global_limit");
        }

        // 2) store bulkhead（带 maxStores 防护，避免 map 无限涨）
        StoreEntry entry = perStore.compute(storeId, (k, existing) -> {
            if (existing != null) {
                existing.touch();
                return existing;
            }
            if (perStore.size() >= maxStores) {
                return null; // 触发“无法创建新 store limiter”
            }
            return new StoreEntry(new Semaphore(perStoreLimit, true));
        });

        if (entry == null) {
            global.release();
            return Permit.denied("store_registry_full");
        }

        if (!entry.sem.tryAcquire()) {
            global.release();
            entry.touch();
            return Permit.denied("store_limit");
        }

        entry.touch();
        return new Permit(true, storeId, global, entry.sem, null);
    }

    /**
     * 定期清理：移除长时间未使用且完全空闲的 store limiter
     */
    public int cleanupIdleStores() {
        long now = System.currentTimeMillis();
        int removed = 0;

        Iterator<Map.Entry<String, StoreEntry>> it = perStore.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, StoreEntry> e = it.next();
            StoreEntry entry = e.getValue();

            boolean idleTooLong = now - entry.lastUsedMillis > storeIdleTtl.toMillis();
            boolean fullyIdle = entry.sem.availablePermits() == perStoreLimit; // 没有正在运行的请求

            if (idleTooLong && fullyIdle) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    private static class StoreEntry {
        final Semaphore sem;
        volatile long lastUsedMillis;

        StoreEntry(Semaphore sem) {
            this.sem = sem;
            touch();
        }

        void touch() {
            this.lastUsedMillis = System.currentTimeMillis();
        }
    }

    public record Permit(
            boolean acquired,
            String storeId,
            Semaphore globalSem,
            Semaphore storeSem,
            String denyReason
    ) {
        public static Permit denied(String reason) {
            return new Permit(false, null, null, null, reason);
        }

        public void release() {
            if (!acquired) return;
            // 释放顺序无强约束，这里先 store 后 global
            storeSem.release();
            globalSem.release();
        }
    }
}
