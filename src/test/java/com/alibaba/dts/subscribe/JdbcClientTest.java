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
