package com.kafka.read;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entities.ModelInfo;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;

public class ParsingBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String value = tuple.getStringByField("value");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            ModelInfo modelInfo = objectMapper.readValue(value, ModelInfo.class);
            String modelId = modelInfo.getModelId();
            collector.emit(new Values(modelId, value));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("modelId", "modelInfo"));
    }
}