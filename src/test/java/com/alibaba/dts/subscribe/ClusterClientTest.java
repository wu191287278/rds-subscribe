package com.alibaba.dts.subscribe;

import com.alibaba.dts.subscribe.utils.SqlUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Arrays;

public class ClusterClientTest {

    public static void main(String[] args) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(3000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        curatorFramework.start();

        RdsSubscribeProperties rdsSubscribeProperties = new RdsSubscribeProperties()
                .setBrokers("dts-cn-beijing.aliyuncs.com:18001")
                .setTopic("topic")
                .setGroupId("")
                .setUsername("")
                .setPassword("")
                .setStartTimeMs(System.currentTimeMillis());
        Client client = new Client(rdsSubscribeProperties, Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(Row row) {
                System.err.println(SqlUtils.toSql(row));
            }
        }));
        ClusterClient clusterClient = new ClusterClient(client, curatorFramework);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            clusterClient.close();
            curatorFramework.close();
        }));
        clusterClient.start();

    }

}
