package com.alibaba.dts.subscribe;

import com.alibaba.dts.subscribe.positioner.FilePosition;
import com.alibaba.dts.subscribe.utils.SqlUtils;

import java.util.Arrays;

/**
 * {
 *   "brokers" : "dts-cn-beijing.aliyuncs.com:18001",
 *   "topic" : "test",
 *   "groupId" : "dtsqcuo6zm518pgbjj",
 *   "username" : "test",
 *   "password" : "test",
 *   "startTimeMs" : 1560925348293,
 *   "offset" : 14540205,
 *   "sessionTimeoutMs" : 30000,
 *   "autoCommitIntervalMs" : 30000
 * }
 */
public class FileClientTest {

    public static void main(String[] args) {
        Client client = new Client(new FilePosition("./position.json"), Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(Row row) {
                System.err.println(SqlUtils.toSql(row));
            }
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        client.start();

    }

}
