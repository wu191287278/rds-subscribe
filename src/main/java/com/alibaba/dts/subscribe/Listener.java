package com.alibaba.dts.subscribe;

public interface Listener {

    void onNext(Row row);

    void onError(Exception e);

    boolean match(Row row);

    void onFinish();

    void close();


}
