package com.alibaba.dts.subscribe;

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


    public String getBrokers() {
        return brokers;
    }

    public RdsSubscribeProperties setBrokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public RdsSubscribeProperties setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public RdsSubscribeProperties setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public RdsSubscribeProperties setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RdsSubscribeProperties setPassword(String password) {
        this.password = password;
        return this;
    }

    public Long getStartTimeMs() {
        return startTimeMs;
    }

    public RdsSubscribeProperties setStartTimeMs(Long startTimeMs) {
        this.startTimeMs = startTimeMs;
        return this;
    }

    public Long getOffset() {
        return offset;
    }

    public RdsSubscribeProperties setOffset(Long offset) {
        this.offset = offset;
        return this;
    }

    public Integer getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public RdsSubscribeProperties setSessionTimeoutMs(Integer sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        return this;
    }

    public Integer getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public RdsSubscribeProperties setAutoCommitIntervalMs(Integer autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        return this;
    }
}
