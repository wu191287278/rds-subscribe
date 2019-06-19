package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;

public class JdbcPositioner implements Positioner {

    private DataSource dataSource;

    private static final String TABLE_NAME = "rds_subscribe_offset";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE `" + TABLE_NAME + "` (" +
            "  `id` varchar(32) NOT NULL," +
            "  `brokers` varchar(128) DEFAULT NULL," +
            "  `group_id` varchar(128) NOT NULL," +
            "  `topic` varchar(128) NOT NULL," +
            "  `username` varchar(128) NOT NULL," +
            "  `password` varchar(128) NOT NULL," +
            "  `startTime` datetime DEFAULT NULL," +
            "  `offset` bigint(19) DEFAULT NULL," +
            "  `session_timeout_ms` int(5) DEFAULT '30000'," +
            "  `auto_commit_interval_ms` int(5) DEFAULT '30000'," +
            "  PRIMARY KEY (`id`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME + "`" +
            "(`id`,`brokers`, `group_id`, `topic`, `username`, `password`, `startTime`, `offset`, `session_timeout_ms`, `auto_commit_interval_ms`) " +
            "VALUES " +
            "(?,?,?,?,?,?,?,?,?,?)";

    private static final String SELECT_SQL = "SELECT " +
            "`id`, `brokers`, `group_id`, `topic`, `username`, `password`, `startTime`, `offset`, `session_timeout_ms`, `auto_commit_interval_ms`" +
            "FROM `" + TABLE_NAME + "`" +
            "WHERE `id`=?";

    private String id;

    public JdbcPositioner(DataSource dataSource, String id) {
        this.dataSource = dataSource;
        this.id = id;
        init();
    }

    private void init() {
        createTable(TABLE_NAME);
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
                    return new RdsSubscribeProperties()
                            .setGroupId(groupId)
                            .setTopic(topic)
                            .setUsername(username)
                            .setPassword(password)
                            .setStartTimeMs(startTime.getTime())
                            .setOffset(offset)
                            .setBrokers(brokers)
                            .setSessionTimeoutMs(sessionTimeoutMs)
                            .setAutoCommitIntervalMs(autoCommitIntervalMs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("未在数据库中发现相关groupId");
    }


    public void createTable(String tableName) {
        try (Connection connection = this.dataSource.getConnection();
             ResultSet resultSet = connection.getMetaData().getTables(null, null, tableName, null)) {
            if (resultSet.next()) return;
            try (Statement statement = connection.createStatement();) {
                statement.execute(CREATE_TABLE_SQL);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
