# rds-subscribe

该项目用于简化消费新版阿里云数据订阅工具包


### 数据格式定义
---
```
{
   "database":"db",
   "table":"photo",
   "type":"update",
   "primaryKeys":[
     {
       "name":"id",
       "value":1,
       "type":"int",
       "isPrimaryKey":true
     }
   ],"data":[
     {
       "name":"id",
       "value":1,
       "type":"int",
       "isPrimaryKey":true
     },
     {
       "name":"username",
       "value":"zhangsan",
       "type":"varchar",
       "isPrimaryKey":false
     },
     {
      "name":"created_at",
      "value":14000000000,
      "type":"datetime",
      "isPrimaryKey":false
     }
   ],
   "old":[
       {
          "name":"id",
          "value":1,
          "type":"int",
          "isPrimaryKey":true
        },
        {
          "name":"username",
          "value":"zhangsan",
          "type":"varchar",
          "isPrimaryKey":false
        },
        {
         "name":"created_at",
         "value":14000000000,
         "type":"datetime",
         "isPrimaryKey":false
        }
   ]
}

```

#### 数据格式说明

|名称 | 描述|
|---|---|
|database | 数据库名称|
|table | 表名称|
|type | 事件 insert、update、delete,ddl|
|data | name:名称, type:类型, value:数据 ,isPrimaryKey:主键|
|old | update、delete 更改前的旧值 格式同data一样|



### java 客户端使用
---

#### 依赖
```
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>rds-subscribe</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

#### Quick start

```
package com.alibaba.dts.subscribe;

import com.alibaba.dts.subscribe.utils.SqlUtils;

import java.util.Arrays;

public class MemoryClientTest {

    public static void main(String[] args) {
        RdsSubscribeProperties rdsSubscribeProperties = new RdsSubscribeProperties()
                .setBrokers("dts-cn-beijing.aliyuncs.com:18001")
                .setTopic("test") //阿里云生成的topic
                .setGroupId("dtskcbj68g119o5b49") //阿里云新建消费组的时候生成的id
                .setUsername("test")
                .setPassword("test")
                .setStartTimeMs(System.currentTimeMillis());
        Client client = new Client(rdsSubscribeProperties, Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(Row row) {
                System.err.println(SqlUtils.toSql(row));
            }
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        client.start();
    }

}
```

#### 保存消费偏移量到文件中

```
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
 *   "autoCommitIntervalMs" : 30000,
 *   "pollTimeout" : 1000,
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
        
        //client.reload();//重新从文件中加载配置
        //client.reload(new Date());//从指定时间开始重新消费
    }

}

```

#### 保存消费偏移量到数据库中

建表语句

```
CREATE TABLE `rds_subscribe_offset` (
  `id` varchar(32) NOT NULL COMMENT '唯一id',
  `brokers` varchar(128) DEFAULT NULL COMMENT '阿里云提供的消费地址',
  `group_id` varchar(128) NOT NULL COMMENT '阿里云新建消费组的时候生成的id',
  `topic` varchar(128) NOT NULL COMMENT '阿里云生成的topic',
  `username` varchar(128) NOT NULL,
  `password` varchar(128) NOT NULL,
  `startTime` datetime DEFAULT NULL COMMENT '启动时从这个时间开始消费',
  `offset` bigint(19) DEFAULT NULL COMMENT 'kafka消费偏移量, 该值仅仅是为了记录.',
  `session_timeout_ms` int(5) DEFAULT '30000' COMMENT '会话超时时间',
  `auto_commit_interval_ms` int(5) DEFAULT '30000' COMMENT '多久自动保存一次偏移量到数据库中',
  `poll_timeout` int(5) DEFAULT '1000',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

```
package com.alibaba.dts.subscribe;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.dts.subscribe.positioner.JdbcPositioner;
import com.alibaba.dts.subscribe.positioner.Positioner;
import com.alibaba.dts.subscribe.utils.SqlUtils;

import javax.sql.DataSource;
import java.util.Arrays;

public class JdbcClientTest {
    public static void main(String[] args) {
        Positioner positioner = new JdbcPositioner(dataSource(), "1");
        Client client = new Client(positioner, Arrays.asList(new AbstractListener() {
            @Override
            public void onNext(Row row) {
                System.err.println(SqlUtils.toSql(row));
            }
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        client.start();
    }

    private static DataSource dataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl("jdbc:mysql://localhost/data_sync");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("root");
        druidDataSource.setMaxActive(5);
        return druidDataSource;
    }
}


```

#### 集群


```
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

        RdsSubscribeProperties rdsSubscribeProperties = new RdsSubscribeProperties()
                .setBrokers("dts-cn-beijing.aliyuncs.com:18001")
                .setTopic("")
                .setGroupId("groupId")
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
        curatorFramework.start();
        clusterClient.start();

    }

}

```
