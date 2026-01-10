package com.agent.demo.tools;

import java.util.Map;

public record ToolResult(String toolName, String summary, Map<String, Object> metrics) {
}
