package com.alibaba.dts.subscribe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractListener implements Listener {

    private static final Logger log = LoggerFactory.getLogger(AbstractListener.class);

    @Override
    public abstract void onNext(List<Row> rows) throws Exception;


    @Override
    public void onError(Exception e) {
        log.warn(e.getMessage(), e);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean match(Row row) {
        return true;
    }

    @Override
    public void onFinish() {

    }
}
