package com.alibaba.dts.subscribe;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class AbstractConverterListener<T> extends AbstractListener {

    private Class<T> clazz;

    @SuppressWarnings(value = "unchecked")
    public AbstractConverterListener() {
        if (this.getClass().getGenericSuperclass() instanceof ParameterizedType) {
            ParameterizedType superGclass = (ParameterizedType) this.getClass().getGenericSuperclass();
            Type[] arguments = superGclass.getActualTypeArguments();
            if (arguments.length > 0) {
                this.clazz = (Class<T>) arguments[0];
            }
        }
    }

    @Override
    public void onNext(Row row) {
        try {
            T data = null;
            T old = null;
            if (row.getType() == Row.Type.insert || row.getType() == Row.Type.update) {
                data = getSerialization().parseObject(row.getData(), clazz);
            }
            if (row.getType() == Row.Type.update || row.getType() == Row.Type.delete) {
                old = getSerialization().parseObject(row.getOld(), clazz);
            }
            doNext(row, data, old);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void doNext(Row row, T data, T old);


    public Serialization getSerialization() {
        return JacksonSerialization.DEFAULT_INSTANCE;
    }


}
