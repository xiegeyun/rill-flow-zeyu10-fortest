package com.weibo.rill.flow.olympicene.traversal.helper;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class TracerHelper {
    private RedisClient redisClient;
    @Getter
    private Tracer tracer;

    public TracerHelper(RedisClient redisClient, Tracer tracer) {
        this.redisClient = redisClient;
        this.tracer = tracer;
    }

    // Redis key 前缀
    private static final String TRACE_KEY_PREFIX = "rill_flow_trace_";
    // 设置合适的过期时间（例如24小时）
    private static final int TRACE_EXPIRE_SECONDS = 2 * 60 * 60;

    public void saveSpanContext(String executionId, String taskId, String traceId, String spanId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            JSONObject traceInfo = new JSONObject();
            traceInfo.put("traceId", traceId);
            traceInfo.put("spanId", spanId);
            traceInfo.put("timestamp", String.valueOf(System.currentTimeMillis()));

            redisClient.set(key, traceInfo.toJSONString());
            // 设置过期时间
            redisClient.expire(key, TRACE_EXPIRE_SECONDS);
        } catch (Exception e) {
            log.error("Failed to save span context to Redis for task: {}, ", taskId, e);
        }
    }

    public SpanContext loadSpanContext(String executionId, String taskId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            String traceInfoString = redisClient.get(key);

            if (traceInfoString == null || traceInfoString.isEmpty()) {
                return null;
            }

            JSONObject traceInfo = JSONObject.parseObject(traceInfoString);

            String traceId = traceInfo.getString("traceId");
            String spanId = traceInfo.getString("spanId");

            return SpanContext.create(
                    traceId,
                    spanId,
                    TraceFlags.getSampled(),
                    TraceState.getDefault()
            );
        } catch (Exception e) {
            log.error("Failed to load span context from Redis for task: {}", taskId, e);
            return null;
        }
    }
}
