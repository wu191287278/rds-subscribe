package com.alibaba.dts.subscribe;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RdsSubscribeProperties {

    /**
     * 阿里云数据订阅地址
     */
    private String brokers;


    /**
     * topic
     */
    private String topic;


    /**
     * 消费组id
     */
    private String groupId;

    /**
     * 消费组的用户名
     */
    private String username;

    /**
     * 消费组的密码
     */
    private String password;


    /**
     * 开始时间
     */
    private Long startTimeMs;

    /**
     * kafka 偏移量
     */
    private Long offset;


    /**
     * session 超时时间
     */
    private Integer sessionTimeoutMs = 30000;

    /**
     * 多长时间自动commit一次
     */
    private Integer autoCommitIntervalMs = 30000;


}
