package com.alibaba.dts.subscribe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractListener implements Listener {

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    @Override
    public abstract void onNext(Row row);


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
