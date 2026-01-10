package com.agent.demo.tools;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class ToolRegistry {
    private final Map<String, Tool> toolsMap;

    public ToolRegistry(List<Tool> tools) {
        this.toolsMap = tools.stream().collect(Collectors.toMap(Tool::name, Function.identity()));
    }

    public Tool get(String toolName) {
        Tool tool = toolsMap.get(toolName);
        if (Objects.isNull(tool)) {
            throw new IllegalArgumentException("Unknown Tools: " + tool);
        }
        return tool;
    }
}
