package com.kafka.tools;

import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;

public class RoundRobinTopicSelector implements KafkaTopicSelector {
    private List<String> topics;
    private int index;

    public RoundRobinTopicSelector(String... topics) {
        this.topics = Arrays.asList(topics);
        this.index = 0;
    }

    @Override
    public String getTopic(Tuple tuple) {
        String topic = topics.get(index);
        index = (index + 1) % topics.size();
        return topic;
    }
}
