package com.kafka.read;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.Set;

public class DeduplicationBolt extends BaseBasicBolt {
    private Set<String> seenModelIds = new HashSet<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String modelId = tuple.getStringByField("modelId");
        if (!seenModelIds.contains(modelId)) {
            seenModelIds.add(modelId);
            collector.emit(tuple.getValues());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("modelId", "modelInfo"));
    }
}
