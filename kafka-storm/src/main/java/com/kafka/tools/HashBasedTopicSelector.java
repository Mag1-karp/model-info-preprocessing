package com.kafka.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entities.ModelInfo;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

public class HashBasedTopicSelector implements KafkaTopicSelector {

    private String[] topics;

    public HashBasedTopicSelector(String... topics) {
        this.topics = topics;
    }

    @Override
    public String getTopic(Tuple tuple) {
        String message = tuple.getStringByField("message");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            ModelInfo modelInfo = objectMapper.readValue(message, ModelInfo.class);
            String modelId = modelInfo.getModelId();
            int index = Math.abs(modelId.hashCode()) % topics.length;
            System.out.println("Model ID: " + modelId + " -> Topic: " + topics[index]);
            return topics[index];
        } catch (IOException e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }
}