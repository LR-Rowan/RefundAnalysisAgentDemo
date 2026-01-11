package com.agent.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "agent.timeouts")
public class AgentTimeoutProperties {
    /**
     * 单个 tool 的最长执行时间（避免工具挂死/慢 I/O）
     */
    private Duration tool = Duration.ofSeconds(10);

    /**
     * LLM 流式 idle 超时：持续没有新 token/delta 的时间上限
     */
    private Duration llmIdle = Duration.ofSeconds(15);

    /**
     * LLM 流式最大总时长：防止长时间输出/网络抖动拖挂
     */
    private Duration llmMax = Duration.ofSeconds(60);

    /**
     * 单次 run 的全链路兜底上限（从订阅开始计时）
     */
    private Duration run = Duration.ofSeconds(180);

    public Duration getTool() { return tool; }
    public void setTool(Duration tool) { this.tool = tool; }

    public Duration getLlmIdle() { return llmIdle; }
    public void setLlmIdle(Duration llmIdle) { this.llmIdle = llmIdle; }

    public Duration getLlmMax() { return llmMax; }
    public void setLlmMax(Duration llmMax) { this.llmMax = llmMax; }

    public Duration getRun() { return run; }
    public void setRun(Duration run) { this.run = run; }
}