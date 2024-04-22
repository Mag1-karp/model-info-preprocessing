package com.kafka.read;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class HiveBolt extends BaseBasicBolt {

    private Driver hiveDriver;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        HiveConf hiveConf = new HiveConf();
        //hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://CurriculumService:9083");
        SessionState.start(hiveConf);
        hiveDriver = new Driver(hiveConf);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String modelId = tuple.getStringByField("modelId");
        String modelInfo = tuple.getStringByField("modelInfo");

        String sql = "INSERT INTO table model_info_table VALUES ('" + modelId + "', '" + modelInfo + "')";

        try {
            hiveDriver.run(sql);
        } catch (Exception e) {
            throw new RuntimeException("Error inserting data into Hive", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 这个Bolt不发射任何数据，所以不需要声明任何字段
    }
}
