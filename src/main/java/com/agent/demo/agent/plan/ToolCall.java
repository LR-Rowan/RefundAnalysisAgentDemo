package com.agent.demo.agent.plan;

import java.util.Map;

public record ToolCall(String name, Map<String, Object> args) {
}
