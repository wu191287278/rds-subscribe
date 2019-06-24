package com.alibaba.dts.subscribe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterClient {

    private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

    private Client client;

    private CuratorFramework curatorFramework;

    private LeaderLatch leaderLatch;

    private String path = "/rds-subscribe";

    private String leaderPath = path + "/leader";

    private AtomicBoolean isClosed = new AtomicBoolean(true);

    private AtomicBoolean isLeader = new AtomicBoolean(false);

    private CountDownLatch waitCounter = new CountDownLatch(1);


    public ClusterClient(Client client, CuratorFramework curatorFramework) {
        this.client = client;
        this.curatorFramework = curatorFramework;
        init();
    }

    private void init() {
        this.leaderLatch = new LeaderLatch(this.curatorFramework, this.leaderPath, this.client.getId());
    }


    public void start() {
        try {
            asyncStart();
            this.waitCounter.await();
        } catch (InterruptedException e) {
            log.warn(e.getMessage(), e);
        }
    }


    public void asyncStart() {
        if (!this.isClosed.get()) {
            throw new RuntimeException("This consumer has already been started.");
        }

        this.leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                client.asyncStart();
                isLeader.set(true);
            }

            @Override
            public void notLeader() {
                client.close();
                isLeader.set(false);
            }
        });

        try {
            this.leaderLatch.start();
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
        this.isClosed.set(false);
    }

    public void close() {
        if (this.leaderLatch != null) {
            try {
                this.leaderLatch.close();
            } catch (IOException e) {
                log.warn(e.getMessage(), e);
            }
        }
        this.isClosed.set(true);
        this.waitCounter.countDown();
    }


    /**
     * 从指定时间开始消费
     *
     * @param startTime 指定时间
     */
    public void reload(Date startTime) {
        checkIsLeader();
        this.client.reload(startTime);
    }

    /**
     * 重新加载配置文件
     */
    public void reload() {
        checkIsLeader();
        this.client.reload();
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    private void checkIsLeader() {
        if (!isLeader.get()) {
            throw new RuntimeException("This consumer is not leader");
        }
    }


}
