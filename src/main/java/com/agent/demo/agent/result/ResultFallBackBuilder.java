package com.agent.demo.agent.result;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.tools.ToolResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 兜底结果: 超时/失败也能落盘 + 返回 result + result_meta
 */
public class ResultFallBackBuilder {
    @SuppressWarnings("unchecked")
    public static AgentResult build(AgentContext ctx, List<ToolResult> tools) {
        List<String> findings = new ArrayList<>();
        List<RootCause> causes = new ArrayList<>();
        List<ActionItem> actions = new ArrayList<>();

        ToolResult refund = tools.stream().filter(t -> "refund_rate".equals(t.toolName())).findFirst().orElse(null);
        ToolResult logistics = tools.stream().filter(t -> "logistics_delay".equals(t.toolName())).findFirst().orElse(null);

        if (refund != null) {
            findings.add("退款概览：" + refund.summary());
            causes.add(new RootCause("退款原因分布", "Top原因集中（见工具输出）", "evidence: refund_rate.summary"));
        }
        if (logistics != null) {
            findings.add("物流概览：" + logistics.summary());
            causes.add(new RootCause("物流延迟", "可能导致退款上升（见工具输出）", "evidence: logistics_delay.summary"));
        }

        actions.add(new ActionItem(
                "建立退款/物流延迟的日度监控与告警",
                "Data/Operations",
                "P1",
                "本周内",
                "看板上线并能追踪退款原因与延迟率趋势"
        ));

        // evidence：把工具原样塞进去，保证可审计
        Map<String, Object> evidence = Map.of("tools", tools);

        return new AgentResult(
                ctx.storeId(),
                ctx.windowDays(),
                findings,
                causes,
                actions,
                evidence
        );
    }
}
