package com.alibaba.dts.subscribe;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;

public abstract class AbstractConverterListener<T> extends AbstractListener {

    private Class<T> clazz;

    @SuppressWarnings(value = "unchecked")
    public AbstractConverterListener() {
        if (this.getClass().getGenericSuperclass() instanceof ParameterizedType) {
            ParameterizedType superGclass = (ParameterizedType) this.getClass().getGenericSuperclass();
            Type[] arguments = superGclass.getActualTypeArguments();
            if (arguments.length > 0) {
                this.clazz = (Class<T>) arguments[0];
            } else {
                this.clazz = (Class<T>) HashMap.class;
            }
        }
    }

    @Override
    public void onNext(Row row) throws Exception {
        T data = null;
        T old = null;
        if (row.getType() == Row.Type.insert || row.getType() == Row.Type.update) {
            data = getSerialization().parseObject(row.getData(), clazz);
        }
        if (row.getType() == Row.Type.update || row.getType() == Row.Type.delete) {
            old = getSerialization().parseObject(row.getOld(), clazz);
        }
        doNext(row, data, old);
    }

    public abstract void doNext(Row row, T data, T old) throws Exception;


    public Serialization getSerialization() {
        return JacksonSerialization.DEFAULT_INSTANCE;
    }


}
