package com.agent.demo.tools;

import com.agent.demo.agent.AgentContext;
import reactor.core.publisher.Mono;

public interface Tool {
    String name();
    Mono<ToolResult> execute(AgentContext context);
}
