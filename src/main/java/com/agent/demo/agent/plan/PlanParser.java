package com.agent.demo.agent.plan;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * PlanParse - Jackson 解析 + fallback
 */
public class PlanParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Plan parse(String json) {
        try {
            return MAPPER.readValue(json, Plan.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid_plan_json: " + e.getMessage(), e);
        }
    }
}
