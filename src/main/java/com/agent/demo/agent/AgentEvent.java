package com.agent.demo.agent;

public record AgentEvent(String type, String payload) {
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
}
