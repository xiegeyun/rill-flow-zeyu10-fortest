/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.olympicene.traversal.mappings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.weibo.rill.flow.interfaces.model.mapping.Mapping;
import com.weibo.rill.flow.olympicene.traversal.serialize.DAGTraversalSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class JSONPathInputOutputMapping implements InputOutputMapping, JSONPath {
    Configuration conf = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();
    private static final Pattern JSONPATH_PATTERN = Pattern.compile("\\[(.*?)]");

    @Value("${rill.flow.function.trigger.uri}")
    private String rillFlowFunctionTriggerUri;
    @Value("${rill.flow.server.host}")
    private String serverHost;

    @Override
    public void mapping(Map<String, Object> context, Map<String, Object> input, Map<String, Object> output, List<Mapping> rules) {
        if (CollectionUtils.isEmpty(rules) || context == null || input == null || output == null) {
            return;
        }

        Map<String, Object> map = new HashMap<>();
        map.put("context", context);
        map.put("input", input);
        map.put("output", output);

        List<Mapping> mappingRules = rules.stream()
                .filter(rule -> (StringUtils.isNotBlank(rule.getSource()) || StringUtils.isNotBlank(rule.getTransform()))
                        && StringUtils.isNotBlank(rule.getTarget()))
                .toList();
        for (Mapping mapping : mappingRules) {
            boolean intolerance = mapping.getTolerance() != null && !mapping.getTolerance();
            try {
                String source = mapping.getSource();
                Object sourceValue = null;
                if (source != null) {
                    String[] infos = source.split("\\.");
                    if (source.startsWith("$.tasks.") && infos.length > 3) {
                        String taskName = infos[2];
                        String key = infos[3];
                        if (key.equals("trigger_url") || key.startsWith("trigger_url?")) {
                            sourceValue = serverHost + rillFlowFunctionTriggerUri + "?execution_id=" + context.get("flow_execution_id") + "&task_name=" + taskName;
                            String[] queryInfos = source.split("\\?");
                            if (queryInfos.length > 0) {
                                sourceValue += '&' + queryInfos[1];
                            }
                        }
                    } else {
                        sourceValue = source.startsWith("$") ? getValue(map, source) : parseSource(source);
                    }
                }

                Object transformedValue = transformSourceValue(sourceValue, context, input, output, mapping.getTransform());

                if (transformedValue != null) {
                    map = setValue(map, transformedValue, mapping.getTarget());
                }
            } catch (Exception e) {
                log.warn("mapping fails, intolerance:{}, mapping:{} due to {}", intolerance, mapping, e.getMessage());
                if (intolerance) {
                    throw e;
                }
            }
        }
    }

    public Object transformSourceValue(Object sourceValue, Map<String, Object> context, Map<String, Object> input,
                                        Map<String, Object> output, String transform) {
        if (StringUtils.isBlank(transform)) {
            return sourceValue;
        }

        Map<String, Object> env = Maps.newHashMap();
        env.put("source", sourceValue);
        env.put("context", context);
        env.put("input", input);
        env.put("output", output);
        return doTransform(transform, env);
    }

    /**
     * <pre>
     * AviatorEvaluator.execute每次运行时会加载临时类
     *   如：[Loaded Script_1638847124088_67/93314457 from com.googlecode.aviator.Expression]
     * 长时间大量使用aviator会导致Metaspace oom
     *   如：java.lang.OutOfMemoryError: Compressed class space
     * 若长期大量使用aviator转换sourceValue 则建议使用guava等本地缓存框架缓存aviator表达式编译结果
     *   如：Expression expression = AviatorEvaluator.compile(transform) 缓存该表达式
     *      expression.execute(env)
     * </pre>
     */
    public Object doTransform(String transform, Map<String, Object> env) {
        return AviatorEvaluator.execute(transform, env);
    }

    public static Object parseSource(String source) {
        if (StringUtils.isBlank(source)) {
            return source;
        }

        String sourceTrim = source.trim();
        try {
            JsonNode jsonNode = DAGTraversalSerializer.MAPPER.readTree(sourceTrim);
            if (jsonNode != null && jsonNode.isObject()) {
                return DAGTraversalSerializer.MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
                });
            }
            if (jsonNode != null && jsonNode.isArray()) {
                return DAGTraversalSerializer.MAPPER.convertValue(jsonNode, new TypeReference<List<Object>>() {
                });
            }
        } catch (Exception e) {
            // not json ignore
        }

        String sourceLowerCase = sourceTrim.toLowerCase();
        if ("true".equals(sourceLowerCase)) {
            return true;
        }
        if ("false".equals(sourceLowerCase)) {
            return false;
        }

        try {
            BigDecimal bigDecimal = new BigDecimal(sourceTrim);
            if (sourceTrim.contains(".")) {
                return bigDecimal.doubleValue();
            } else {
                return bigDecimal.longValue();
            }
        } catch (Exception e) {
            // not number ignore
        }

        return source;
    }

    @Override
    public Object getValue(Map<String, Object> map, String path) {
        try {
            return JsonPath.using(conf).parse(map).read(path);
        } catch (InvalidPathException e) {
            return null;
        }
    }

    @Override
    public Map<String, Object> setValue(Map<String, Object> map, Object value, String path) {
        if (map == null) {
            return null;
        }

        String jsonPath = JsonPath.compile(path).getPath();
        List<String> jsonPathParts = new ArrayList<>();
        Matcher matcher = JSONPATH_PATTERN.matcher(jsonPath);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                jsonPathParts.add(matcher.group(1));
            }
        }

        Object current = map;
        for (int i = 0; i < jsonPathParts.size() - 1; i++) {
            String part = jsonPathParts.get(i);
            if (part.startsWith("\"") || part.startsWith("'")) {
                part = part.substring(1, part.length() - 1);
            }
            if (current instanceof Map) {
                current = processMapJsonPathPart(current, part, jsonPathParts, i);
            } else if (current instanceof List) {
                current = processListJsonPathPart(current, part, jsonPathParts, i);
            }
        }

        return JsonPath.using(conf).parse(map).set(path, value).json();
    }

    private Object processListJsonPathPart(Object current, String part, List<String> jsonPathParts, int i) {
        List<Object> listCurrent = (List<Object>) current;
        int index = Integer.parseInt(part);
        Object insertPosition = listCurrent.get(index);
        if (jsonPathParts.get(i + 1).matches("\\d+")) {
            // 1. 下一个元素是数字，也就是数组的索引，所以需要创建数组并且填充到索引位置
            List<Object> nextArray = createAndFillNextArrayPart(insertPosition, jsonPathParts, i);
            listCurrent.set(index, nextArray);
        } else if (i + 1 < jsonPathParts.size() && insertPosition == null) {
            // 2. 下一个元素不是数字，则创建 map
            listCurrent.set(index, new HashMap<>());
        }
        return listCurrent.get(index);
    }

    private Object processMapJsonPathPart(Object current, String part, List<String> jsonPathParts, int i) {
        Map<String, Object> mapCurrent = (Map<String, Object>) current;
        Object currentValue = mapCurrent.get(part);
        if (jsonPathParts.get(i + 1).matches("\\d+")) {
            List<Object> nextArray = createAndFillNextArrayPart(currentValue, jsonPathParts, i);
            mapCurrent.put(part, nextArray);
        } else if (i + 1 < jsonPathParts.size() && currentValue == null) {
            mapCurrent.put(part, new HashMap<>());
        }
        return mapCurrent.get(part);
    }

    /**
     * 为下一个元素创建数组类型对象，并用 null 值填充指定元素个数
     */
    private List<Object> createAndFillNextArrayPart(Object nextPart, List<String> jsonPathParts, int i) {
        List<Object> nextArray;
        if (nextPart instanceof List) {
            nextArray = (List<Object>) nextPart;
        } else {
            nextArray = new ArrayList<>();
        }
        int nextIndex = Integer.parseInt(jsonPathParts.get(i + 1));
        for (int j = nextArray.size(); j <= nextIndex; j++) {
            nextArray.add(null);
        }
        return nextArray;
    }

    @Override
    public Map<String, Map<String, Object>> delete(Map<String, Map<String, Object>> map, String path) {
        if (map == null) {
            return null;
        }

        return JsonPath.using(conf).parse(map).delete(path).json();
    }
}
