package com.agent.demo.agent.plan;

import com.agent.demo.agent.AgentContext;
import com.agent.demo.llm.OpenAIClient;
import com.agent.demo.llm.ResponsesTextExtractor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 产出 JSON Plan
 */
@Service
public class Planner {
    private final OpenAIClient openAIClient;

    public Planner(OpenAIClient openAIClient) {
        this.openAIClient = openAIClient;
    }

    public Mono<Plan> plan(AgentContext ctx) {
        String prompt = buildPlannerPrompt(ctx);

        return openAIClient.callOnce(prompt)
                .map(ResponsesTextExtractor::extractOutputText)
                .map(Planner::extractJsonBlock)
                .map(String::trim)
                .map(PlanParser::parse);
    }

    private static String extractJsonBlock(String s) {
        if (Objects.isNull(s)) {
            return "";
        }
        int a = s.indexOf("<json>");
        int b = s.indexOf("</json>");
        if (a >= 0 && b > a) {
            return s.substring(a + 6, b).trim();
        }
        return s.trim();
    }

    private String buildPlannerPrompt(AgentContext ctx) {
        return """
                你是电商运营分析 Agent 的 Planner。
                你必须只输出一个 JSON，并且必须包裹在 <json> 与 </json> 标签内。
                除 <json>...</json> 外，禁止输出任何字符（包括空格、换行、Markdown）。
                
                可用工具：
                - refund_rate: 读取退款明细，输出退款单数/金额/Top原因/趋势
                - logistics_delay: 读取物流聚合数据，输出延迟率/Top承运商/Top城市
                
                约束：
                - tools 字段是数组，元素包含 name 和 args
                - args 至少包含 windowDays（整数）
                - tool name 必须来自可用工具列表
                
                示例（仅示例，不要照抄内容）：
                <json>
                {"tools":[{"name":"refund_rate","args":{"windowDays":7}},{"name":"logistics_delay","args":{"windowDays":7}}]}
                </json>
                
                输入：
                storeId=%s
                query=%s
                windowDays=%d
                
                现在输出：
                """.formatted(ctx.storeId(), ctx.query(), ctx.windowDays());
    }
}
