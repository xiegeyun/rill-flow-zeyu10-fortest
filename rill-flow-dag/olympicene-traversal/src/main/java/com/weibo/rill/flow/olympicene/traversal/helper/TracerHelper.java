package com.weibo.rill.flow.olympicene.traversal.helper;

import com.alibaba.fastjson.JSONObject;
import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
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

    public void removeSpanContext(String executionId, String taskId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            redisClient.del(key.getBytes());
        } catch (Exception e) {
            log.error("Failed to remove span context from Redis for task: {}", taskId, e);
        }
    }

    public void saveContext(String executionId, String taskId, Context parentContext, Span currentSpan) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            JSONObject contextInfo = new JSONObject();
            SpanContext spanContext = currentSpan.getSpanContext();
            contextInfo.put("traceId", spanContext.getTraceId());
            contextInfo.put("spanId", spanContext.getSpanId());
            contextInfo.put("traceFlags", spanContext.getTraceFlags().asHex());
            contextInfo.put("timestamp", String.valueOf(System.currentTimeMillis()));

            redisClient.set(key, contextInfo.toJSONString());
            redisClient.expire(key, TRACE_EXPIRE_SECONDS);
        } catch (Exception e) {
            log.error("Failed to save context to Redis for task: {}", taskId, e);
        }
    }

    public Span loadSpan(String executionId, String taskId) {
        try {
            String key = TRACE_KEY_PREFIX + executionId + "_" + taskId;
            String contextInfoString = redisClient.get(key);

            if (contextInfoString == null || contextInfoString.isEmpty()) {
                return null;
            }

            JSONObject contextInfo = JSONObject.parseObject(contextInfoString);
            String traceId = contextInfo.getString("traceId");
            String spanId = contextInfo.getString("spanId");
            String traceFlags = contextInfo.getString("traceFlags");

            SpanContext spanContext = SpanContext.createFromRemoteParent(
                    traceId,
                    spanId,
                    TraceFlags.fromHex(traceFlags, 0),
                    TraceState.getDefault()
            );

            return Span.wrap(spanContext);
        } catch (Exception e) {
            log.error("Failed to load span from Redis for task: {}", taskId, e);
            return null;
        } finally {
            removeSpanContext(executionId, taskId);
        }
    }
}
