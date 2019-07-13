package com.alibaba.dts.subscribe;

import com.alibaba.dts.subscribe.utils.SqlUtils;

import java.util.Arrays;
import java.util.List;

public class MemoryClientTest {

    public static void main(String[] args) {
        RdsSubscribeProperties rdsSubscribeProperties = new RdsSubscribeProperties()
                .setBrokers("dts-cn-beijing.aliyuncs.com:18001")
                .setTopic("test")
                .setGroupId("dtskcbj68g119o5b49")
                .setUsername("test")
                .setPassword("test")
                .setStartTimeMs(System.currentTimeMillis());
        Client client = new Client(rdsSubscribeProperties, Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(List<Row> rows) {
                for (Row row : rows) {
                    System.err.println(SqlUtils.toSql(row));
                }
            }
        }));;
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        client.start();
    }

}
