package com.kafka.read;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class HiveBolt extends BaseBasicBolt {

    private Connection connection;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            // 注册 Hive 驱动程序
            // Class.forName("org.apache.hive.jdbc.HiveDriver");

            // 创建 Hive 连接
            String hiveUrl = "jdbc:hive2://CurriculumPractice:10000/default";
            String username = "root";
            String password = "hive";
            connection = DriverManager.getConnection(hiveUrl, username, password);
        } catch (/*ClassNotFoundException | */ SQLException e) {
            throw new RuntimeException("Error connecting to Hive", e);
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String modelId = tuple.getStringByField("modelId");
        String modelInfo = tuple.getStringByField("modelInfo");

        System.out.println("modelId: " + modelId);

        String sql = "INSERT INTO model_info_table VALUES (?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, modelId);
            statement.setString(2, modelInfo);
            statement.executeUpdate();
            System.out.println("Inserted data into Hive");
        } catch (SQLException e) {
            throw new RuntimeException("Error inserting data into Hive", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 这个Bolt不发射任何数据，所以不需要声明任何字段
    }

    @Override
    public void cleanup() {
        // 在关闭 Bolt 之前关闭连接
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
