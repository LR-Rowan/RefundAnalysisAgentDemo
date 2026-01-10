package com.agent.demo.trace;

import java.util.UUID;

public class IdGenerators {
    private IdGenerators() {}

    public static String newTraceId() {
        return "T_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static String newResultId() {
        return "R_" + UUID.randomUUID().toString().replace("-", "");
    }
}
