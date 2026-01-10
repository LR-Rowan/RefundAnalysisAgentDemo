package com.agent.demo.tools;

import com.agent.demo.agent.AgentContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class RefundRateTool implements Tool {

    @Override
    public String name() {
        return "refund_rate";
    }

    @Override
    public Mono<ToolResult> execute(AgentContext ctx) {
        // 读取文件是阻塞 IO：放到 boundedElastic，避免卡 WebFlux 线程
        return Mono.fromCallable(() -> compute(ctx))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private ToolResult compute(AgentContext ctx) throws Exception {
        ClassPathResource resource = new ClassPathResource("data/refunds.csv");
        if (!resource.exists()) {
            throw new IllegalStateException("refunds.csv not found at classpath:data/refunds.csv");
        }

        // 窗口：以“今天”为基准往前 windowDays-1 天
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusDays(Math.max(ctx.windowDays(), 1) - 1L);

        long refundCount = 0;
        double refundAmountSum = 0.0;

        Map<String, Long> reasonCount = new HashMap<>();
        Map<LocalDate, Long> dailyRefundCount = new HashMap<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            String header = br.readLine();
            if (header == null) {
                throw new IllegalStateException("refunds.csv is empty");
            }

            CsvHeaderIndex idx = CsvHeaderIndex.parse(header);

            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;

                List<String> cols = splitCsvLine(line);
                if (cols.size() <= Math.max(idx.date, Math.max(idx.storeId, Math.max(idx.refundAmount, idx.refundReason)))) {
                    // 数据行列数不足，跳过（也可以选择抛错）
                    continue;
                }

                LocalDate date = safeParseDate(cols.get(idx.date));
                if (date == null) continue;

                // 时间窗口过滤
                if (date.isBefore(start) || date.isAfter(end)) continue;

                String storeId = cols.get(idx.storeId).trim();
                if (!Objects.equals(storeId, ctx.storeId())) continue;

                String reason = cols.get(idx.refundReason).trim();
                double amount = safeParseDouble(cols.get(idx.refundAmount));

                refundCount++;
                refundAmountSum += amount;

                reasonCount.merge(reason.isEmpty() ? "未知" : reason, 1L, Long::sum);
                dailyRefundCount.merge(date, 1L, Long::sum);
            }
        }

        List<Map<String, Object>> topReasons = reasonCount.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(3)
                .map(e -> Map.<String, Object>of("reason", e.getKey(), "count", e.getValue()))
                .collect(Collectors.toList());

        // 简单趋势：按日期升序输出（可用于前端图表）
        List<Map<String, Object>> dailyTrend = dailyRefundCount.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> Map.<String, Object>of("date", e.getKey().toString(), "refundCount", e.getValue()))
                .collect(Collectors.toList());

        String summary = refundCount == 0
                ? "窗口期内未发现退款订单"
                : "窗口期内退款单数=" + refundCount + "，退款金额合计=" + round2(refundAmountSum) + "，Top原因=" +
                topReasons.stream().map(x -> x.get("reason") + "(" + x.get("count") + ")").collect(Collectors.joining(", "));

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("windowDays", ctx.windowDays());
        metrics.put("storeId", ctx.storeId());
        metrics.put("refundCount", refundCount);
        metrics.put("refundAmountSum", round2(refundAmountSum));
        metrics.put("topReasons", topReasons);
        metrics.put("dailyTrend", dailyTrend);

        return new ToolResult(name(), summary, metrics);
    }

    /**
     * 解析 CSV header，支持任意列顺序（按列名匹配）
     * 需要列：date, store_id, refund_amount, refund_reason
     */
    private static class CsvHeaderIndex {
        final int date;
        final int storeId;
        final int refundAmount;
        final int refundReason;

        private CsvHeaderIndex(int date, int storeId, int refundAmount, int refundReason) {
            this.date = date;
            this.storeId = storeId;
            this.refundAmount = refundAmount;
            this.refundReason = refundReason;
        }

        static CsvHeaderIndex parse(String headerLine) {
            List<String> headers = splitCsvLine(headerLine).stream()
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());

            int date = indexOf(headers, "date");
            int storeId = indexOf(headers, "store_id");
            int refundAmount = indexOf(headers, "refund_amount");
            int refundReason = indexOf(headers, "refund_reason");

            if (date < 0 || storeId < 0 || refundAmount < 0 || refundReason < 0) {
                throw new IllegalStateException("refunds.csv missing required headers: date, store_id, refund_amount, refund_reason");
            }
            return new CsvHeaderIndex(date, storeId, refundAmount, refundReason);
        }

        private static int indexOf(List<String> headers, String name) {
            for (int i = 0; i < headers.size(); i++) {
                if (Objects.equals(headers.get(i), name)) return i;
            }
            return -1;
        }
    }

    /**
     * 纯 Java CSV 拆分：支持双引号包裹字段（包含逗号）
     * 不支持多行字段（对 demo 足够）
     */
    private static List<String> splitCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // 处理 "" 转义
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (c == ',' && !inQuotes) {
                out.add(sb.toString());
                sb.setLength(0);
                continue;
            }

            sb.append(c);
        }
        out.add(sb.toString());
        return out;
    }

    private static LocalDate safeParseDate(String s) {
        try {
            return LocalDate.parse(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static double safeParseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static double round2(double x) {
        return Math.round(x * 100.0) / 100.0;
    }
}
