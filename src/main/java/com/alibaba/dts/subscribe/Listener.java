package com.alibaba.dts.subscribe;

public interface Listener {

    void onNext(Row row) throws Exception;

    void onError(Exception e);

    boolean match(Row row);

    void onFinish();

    void close();


}
