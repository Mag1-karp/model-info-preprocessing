package com.kafka.read;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 打印从Kafka中获取的数据
 */
public class LogConsoleBolt extends BaseRichBolt {


    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple input) {
        try {
            // String value = input.getStringByField("value");
            String value = input.getStringByField("modelId");
            // System.out.println("received from kafka : "+ value);
            System.out.println("received modelId : "+ value);
            collector.ack(input);
        }catch (Exception e){
            e.printStackTrace();
            collector.fail(input);
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
