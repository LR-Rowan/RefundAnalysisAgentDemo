package com.agent.demo.trace;

public final class TraceKeys {
    /**
     * resultId: 一次 agent run 的结果实体 ID，用于下载与落盘定位
     */
    public static final String TRACE_ID = "traceId";

    /**
     * traceId: 一次请求/链路的追踪 ID（可等于 resultId，也可独立）
     */
    public static final String RESULT_ID = "resultId";

    /**
     *
     */
    public static final String STORE_ID = "storeId";

    /**
     *
     */
    public static final String WINDOW_DAYS = "windowDays";

    private TraceKeys() {}
}
