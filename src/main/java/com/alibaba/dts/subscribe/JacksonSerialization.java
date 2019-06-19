package com.alibaba.dts.subscribe;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JacksonSerialization implements Serialization {

    private ObjectMapper objectMapper;

    private Map<String, String> fieldNameCache = new ConcurrentHashMap<>();


    public JacksonSerialization() {
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }

    public JacksonSerialization(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(Row t) throws IOException {
        return objectMapper.writeValueAsBytes(t);
    }

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Row.class);
    }

    /**
     * 暂时使用 map作为中介进行转换对象
     *
     * @param columns
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    @Override
    public <T> T parseObject(List<Row.Column> columns, Class<T> clazz) throws IOException {
        Map<String, Object> map = parseObject(columns, false);
        return objectMapper.readValue(objectMapper.writeValueAsBytes(map), clazz);
    }

    @Override
    public Map<String, Object> parseObject(List<Row.Column> columns, boolean order) throws IOException {
        Map<String, Object> map;
        if (order) {
            map = new LinkedHashMap<>(columns.size());
        } else {
            map = new HashMap<>(columns.size());
        }
        for (Row.Column column : columns) {
            String name = column.getName();
            String fieldName = fieldNameCache.get(name);
            if (fieldName == null) {
                fieldName = camel(name);
                fieldNameCache.put(name, fieldName);
            }

            Object value = column.getValue();
            map.put(fieldName, value);
        }
        return map;
    }

    /**
     * 下划线转驼峰
     *
     * @param str
     * @return
     */

    private static String camel(String str) {
        str = str.toLowerCase().replace("-", "_");
        while (str.indexOf("_") == 0) {
            str = str.substring(1);
        }
        String[] split = str.split("_+");
        StringBuilder sb = new StringBuilder(split[0]);
        for (int i = 1; i < split.length; i++) {
            String s = split[i];
            sb.append(s.substring(0, 1).toUpperCase());
            if (s.length() > 1) {
                sb.append(s.substring(1));
            }
        }
        return sb.toString();
    }

}
