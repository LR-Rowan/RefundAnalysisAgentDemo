🚀 Refund Analysis Agent Demo
Spring WebFlux + SSE – Production-grade AI Agent Demo

📌 项目简介 | Project Overview
中文：
这是一个基于 Spring WebFlux + Server-Sent Events (SSE) 的工程化 AI Agent Demo，
重点展示 长连接场景下的稳定性、可观测性、限流与超时控制，而不仅是模型调用本身。
项目以「电商退款分析」为示例业务，完整覆盖从 Agent 编排 → Tool 调用 → LLM Streaming → 结果落盘 的全链路。

English:
This project is a production-oriented AI Agent demo built with Spring WebFlux and Server-Sent Events (SSE).
It focuses on engineering concerns such as long-lived connections, observability, rate limiting, and timeouts — not just LLM integration.
The demo uses an e-commerce refund analysis scenario to demonstrate a full Agent pipeline:
planning → tool execution → LLM streaming → result persistence.

🧩 核心能力 | Key Features
🔁 SSE & Streaming
· Unified SSE event protocol: status / tool / delta / result / result_meta / done
· Consistent event envelope with:
        resultId, traceId
        seq, ts, durationMs
        stage, data
· Legacy mode supported (raw SSE without envelope)

❤️ 心跳与生命周期 | Heartbeat & Lifecycle
· Heartbeat comments (:ping) to prevent idle disconnects
· Heartbeat lifecycle bound to business stream
· SSE connection closes naturally after done
· Prevents heartbeat from masking run-level timeouts

🚦 并发限流 | Concurrency Control
· Global and per-store concurrency limits 
· Permit-based acquisition & release 
· agent_inflight metric verified to return to zero after completion / cancel

⏱ 超时策略 | Timeout Strategy
· Tool-level timeout
· LLM idle timeout
· LLM max execution timeout
· Run-level global timeout with fallback result persistence

📊 可观测性 | Observability
· Structured logging with traceId / resultId
· Micrometer metrics:
        inflight gauge
        latency & count per stage
        rate-limited and timeout counters
· Actuator endpoints:
        health / liveness / readiness
        prometheus (secured)

🗄 结果兜底 | Result Persistence
· Results always persisted locally (JSON)
· Downloadable even on timeout or partial failure
· Clear separation between streaming output and final stored result

🔍 验收方式 | Validation
中文：
· 使用 curl -N 验证 SSE 流式输出
· 通过 Ctrl+C 验证 cancel 行为与资源释放
· 使用 Prometheus 指标验证 agent_inflight = 0
· 验证 timeout 场景下结果仍可下载

English:
· SSE streaming verified via curl -N
· Client cancellation tested with Ctrl+C
· Resource cleanup validated via Prometheus (agent_inflight = 0)
· Result persistence confirmed under timeout scenarios

🎯 适用场景 | Use Cases
· Backend / Platform engineering interviews
· SSE & WebFlux production reference
· AI Agent orchestration demos
· Observability and resilience examples

🏁 当前状态 | Current Status
封版版本 / Release Tag: v1.0-demo-ready
This version has passed functional and lifecycle validation and is ready for demo and interview usage.