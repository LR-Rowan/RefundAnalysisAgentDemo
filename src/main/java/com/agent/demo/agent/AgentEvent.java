package com.agent.demo.agent;

public record AgentEvent(
        String type,
        String payload,
        String resultId,
        String traceId
) {
    // ====== 兼容旧调用方式的构造函数 ======
    public AgentEvent(String type, String payload) {
        this(type, payload, null, null);
    }

    // ====== 工厂方法（保持不变） ======
    public static AgentEvent status(String s) {
        return new AgentEvent("status", s);
    }

    public static AgentEvent tool(String json) {
        return new AgentEvent("tool", json);
    }

    public static AgentEvent delta(String s) {
        return new AgentEvent("delta", s);
    }

    public static AgentEvent result(String s) {
        return new AgentEvent("result", s);
    }

    public static AgentEvent resultMeta(String s) {
        return new AgentEvent("result_meta", s);
    }

    // ====== Orchestrator 用来统一打 stamp ======
    public AgentEvent withTrace(String resultId, String traceId) {
        return new AgentEvent(this.type, this.payload, resultId, traceId);
    }
}
