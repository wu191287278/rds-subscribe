package com.alibaba.dts.subscribe;

import com.alibaba.dts.subscribe.utils.SqlUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Arrays;
import java.util.List;

public class ClusterClientTest2 {

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
                .setTopic("cn_beijing_rm_2ze00j9bu2jf28p09_visualchina")
                .setGroupId("dtskcbj68g119o5b49")
                .setUsername("search_test")
                .setPassword("search.123")
                .setStartTimeMs(System.currentTimeMillis());
        Client client = new Client(rdsSubscribeProperties, Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(List<Row> rows) {
                for (Row row : rows) {
                    System.err.println(SqlUtils.toSql(row));
                }
            }
        }));
        ClusterClient clusterClient = new ClusterClient(client, curatorFramework);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            clusterClient.close();
            curatorFramework.close();
        }));
        clusterClient.start();

    }

}
