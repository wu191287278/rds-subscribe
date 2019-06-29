package com.alibaba.dts.subscribe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTimeWindowListener extends AbstractListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractTimeWindowListener.class);

    private int timeout;

    private int maxSize;

    private List<Row> rows = new ArrayList<>(maxSize);

    private ScheduledExecutorService scheduledExecutorService;

    private Semaphore semaphore = new Semaphore(maxSize);


    public AbstractTimeWindowListener() {
        this(1000, 1000);
    }


    public AbstractTimeWindowListener(int maxSize, int timeout) {
        this.maxSize = maxSize;
        this.timeout = timeout;
        init();
    }

    private void init() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutorService.scheduleWithFixedDelay(this::copyOnWrite,
                timeout, timeout,
                TimeUnit.MILLISECONDS);
    }

    private synchronized void copyOnWrite() {
        List<Row> oldRows = this.rows;
        if (oldRows.isEmpty()) return;
        this.rows = new ArrayList<>();
        int size = oldRows.size();
        doNext(oldRows);
        semaphore.release(size);
    }

    @Override
    public void onNext(Row row) {
        try {
            semaphore.acquire(1);
            this.rows.add(row);
        } catch (InterruptedException e) {
            log.warn(e.getMessage(), e);
        } finally {
            semaphore.release();
        }
    }

    protected abstract void doNext(List<Row> rows);

    @Override
    public void close() {
        if (!scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
        copyOnWrite();
    }
}
