package com.alibaba.dts.subscribe;

import java.util.List;

public interface Listener {

    void onNext(List<Row> rows) throws Exception;

    void onError(Exception e);

    boolean match(Row row);

    void close();

}
