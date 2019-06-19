package com.alibaba.dts.subscribe;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Serialization {

    byte[] serialize(Row t) throws IOException;

    Row deserialize(byte[] bytes) throws IOException;

    <T> T parseObject(List<Row.Column> columns, Class<T> clazz) throws IOException;

    Map<String, Object> parseObject(List<Row.Column> columns, boolean order) throws IOException;

    Serialization DEFAULT_INSTANCE = new JacksonSerialization();

}
