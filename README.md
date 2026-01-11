This project demonstrates a production-style WebFlux SSE backend that streams LLM outputs in real time, including error handling, proxy-aware OpenAI integration, and reactive flow orchestration — serving as the foundation for AI Agent services.
该项目展示了一个生产级的 WebFlux SSE 后端，可实时传输 LLM 输出，包括错误处理、代理感知的 OpenAI 集成和响应式流程编排 —— 作为 AI Agent 服务的基础

### 验收测试命令
Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_inflight"

Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_run_total"
Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_run_latency"

Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_rate_limited_total"

Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_tool_total"
Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_tool_latency"

Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_llm_total"
Invoke-RestMethod "http://localhost:8080/actuator/metrics/agent_llm_stream_duration"

### 验收标准：
latency 类指标 MAX > 0
total 类指标 COUNT >= 1
inflight 在 run 完成后 VALUE == 0