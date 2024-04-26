package com.kafka.write;

import com.kafka.tools.HashBasedTopicSelector;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

public class WritingToKafkaApp {

    private static final String BOOTSTRAP_SERVERS = "CurriculumPractice:9092";
    private static final String TOPIC_NAME = "model-info";

    public static void main(String[] args) throws Exception {


        TopologyBuilder builder = new TopologyBuilder();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        KafkaBolt bolt = new KafkaBolt<String, String>()
//                .withProducerProperties(props)
//                .withTopicSelector(new DefaultTopicSelector(TOPIC_NAME))
//                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>());

//        KafkaBolt bolt = new KafkaBolt<String, String>()
//                .withProducerProperties(props)
//                .withTopicSelector(new RoundRobinTopicSelector("model-info", "model-info-1", "model-info-2"))
//                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>());

        KafkaBolt bolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new HashBasedTopicSelector("model-info", "model-info-1", "model-info-2"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>());

        builder.setSpout("sourceSpout", new DataSourceSpout(), 1);
        builder.setBolt("kafkaBolt", bolt, 1).shuffleGrouping("sourceSpout");


        if (args.length > 0 && args[0].equals("cluster")) {
            try {
                StormSubmitter.submitTopology("ClusterWritingToKafkaApp", new Config(), builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LocalWritingToKafkaApp",
                    new Config(), builder.createTopology());
        }
    }
}
