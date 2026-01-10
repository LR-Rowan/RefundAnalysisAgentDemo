package com.agent.demo.agent.result;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record AgentResult(
        String storeId,
        int windowDays,
        List<String> keyFindings,
        List<RootCause> rootCauses,
        List<ActionItem> actions,
        Map<String, Object> evidence
) {}
