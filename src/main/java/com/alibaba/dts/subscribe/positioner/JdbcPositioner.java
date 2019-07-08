package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;

public class JdbcPositioner implements Positioner {

    private DataSource dataSource;

    private static final String TABLE_NAME = "rds_subscribe_offset";


    private static final String REPLACE_SQL = "REPLACE INTO " + TABLE_NAME + "" +
            "(id,brokers, group_id, topic, username, password, startTime, offset, session_timeout_ms, auto_commit_interval_ms,poll_timeout) " +
            "VALUES " +
            "(?,?,?,?,?,?,?,?,?,?,?)";

    private static final String SELECT_SQL = "SELECT " +
            "id, brokers, group_id, topic, username, password, startTime, offset, session_timeout_ms, auto_commit_interval_ms,poll_timeout" +
            "FROM " + TABLE_NAME + "" +
            "WHERE id=?";

    private String id;

    public JdbcPositioner(DataSource dataSource, String id) {
        this.dataSource = dataSource;
        this.id = id;
    }

    @Override
    public void save(RdsSubscribeProperties properties) {
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(REPLACE_SQL)) {
            statement.setString(1, id);
            statement.setString(2, properties.getBrokers());
            statement.setString(3, properties.getGroupId());
            statement.setString(4, properties.getTopic());
            statement.setString(5, properties.getUsername());
            statement.setString(6, properties.getPassword());
            statement.setTimestamp(7, properties.getStartTimeMs() == null
                    ? null : new Timestamp(properties.getStartTimeMs()));
            statement.setLong(8, properties.getOffset());
            statement.setInt(9, properties.getSessionTimeoutMs());
            statement.setInt(10, properties.getAutoCommitIntervalMs());
            statement.setInt(11, properties.getPollTimeout());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RdsSubscribeProperties loadRdsSubscribeProperties() {
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_SQL)) {
            statement.setString(1, this.id);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String id = resultSet.getString(1);
                    String brokers = resultSet.getString(2);
                    String groupId = resultSet.getString(3);
                    String topic = resultSet.getString(4);
                    String username = resultSet.getString(5);
                    String password = resultSet.getString(6);
                    Date startTime = resultSet.getTimestamp(7);
                    Long offset = resultSet.getLong(8);
                    Integer sessionTimeoutMs = resultSet.getInt(9);
                    Integer autoCommitIntervalMs = resultSet.getInt(10);
                    Integer pollTimeout = resultSet.getInt(11);
                    return new RdsSubscribeProperties()
                            .setGroupId(groupId)
                            .setTopic(topic)
                            .setUsername(username)
                            .setPassword(password)
                            .setStartTimeMs(startTime.getTime())
                            .setOffset(offset)
                            .setBrokers(brokers)
                            .setSessionTimeoutMs(sessionTimeoutMs)
                            .setAutoCommitIntervalMs(autoCommitIntervalMs)
                            .setPollTimeout(pollTimeout);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("未在数据库中发现相关记录");
    }


}
