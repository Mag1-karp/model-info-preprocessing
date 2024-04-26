package com.kafka.read;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ReadingFromKafkaApp {

    private static final String BOOTSTRAP_SERVERS = "CurriculumPractice:9092";
    private static final String TOPIC_NAME = "model-info";

    public static void main(String[] args) throws Exception {

        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(BOOTSTRAP_SERVERS, TOPIC_NAME)), 1);
        builder.setBolt("parsingBolt", new ParsingBolt()).shuffleGrouping("kafka_spout");
        builder.setBolt("deduplicationBolt", new DeduplicationBolt()).fieldsGrouping("parsingBolt", new Fields("modelId"));
        // builder.setBolt("logConsoleBolt", new LogConsoleBolt()).shuffleGrouping("deduplicationBolt");
        builder.setBolt("hiveBolt", new HiveBolt()).shuffleGrouping("deduplicationBolt");
        // builder.setBolt( "neo4jBolt", new Neo4jBolt()).shuffleGrouping(  "deduplicationBolt");

        if (args.length > 0 && args[0].equals("cluster")) {
            try {
                StormSubmitter.submitTopology("ClusterReadingFromKafkaApp", new Config(), builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LocalReadingFromKafkaApp",
                    new Config(), builder.createTopology());
        }
    }

    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers, String topic) {
        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }

    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }
}
