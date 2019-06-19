package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * {
 *   "brokers" : "dts-cn-beijing.aliyuncs.com:18001",
 *   "topic" : "topic",
 *   "groupId" : "group",
 *   "username" : "username",
 *   "password" : "password",
 *   "startTimeMs" : 1560925348293,
 *   "sessionTimeoutMs" : 30000,
 *   "autoCommitIntervalMs" : 30000
 * }
 */
public class FilePosition implements Positioner {

    private String path = "./position.json";

    private ObjectMapper objectMapper = new ObjectMapper();


    public FilePosition() {
    }


    public FilePosition(String path) {
        this.path = path;
    }

    @Override
    public void save(RdsSubscribeProperties properties) {
        try (FileOutputStream f = new FileOutputStream(path)) {
            byte[] value = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(properties);
            f.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RdsSubscribeProperties loadRdsSubscribeProperties() {
        try (FileInputStream in = new FileInputStream(path)) {
            return objectMapper.readValue(in, RdsSubscribeProperties.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
