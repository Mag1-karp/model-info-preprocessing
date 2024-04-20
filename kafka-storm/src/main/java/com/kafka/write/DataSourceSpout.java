package com.kafka.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entities.ModelInfo;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 产生词频样本的数据源
 */
public class DataSourceSpout extends BaseRichSpout {

    // private List<String> list = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private String jsonFilePath = "/home/softwares/storm/files/hf_metadata.json";
    private Iterator<ModelInfo> modelInfoIterator;
    private SpoutOutputCollector spoutOutputCollector;
    private String currentId = null;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.modelInfoIterator = productData().iterator();
    }

    @Override
    public void nextTuple() {
        // 模拟产生数据
//        String lineData = productData();
//        spoutOutputCollector.emit(new Values("key",lineData));
//        Utils.sleep(1000);

        if (modelInfoIterator.hasNext()) {
            ModelInfo modelInfo = modelInfoIterator.next();
            // Convert ModelInfo to JSON string
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String lineData = objectMapper.writeValueAsString(modelInfo);
                spoutOutputCollector.emit(new Values("key", lineData));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            Utils.sleep(1000);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("key", "message"));
    }

    /**
     * 从JSON文件中读取数据
     */
    private List<ModelInfo> productData() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // 读取JSON文件并转换为ModelInfo对象列表
            List<ModelInfo> modelInfos = objectMapper.readValue(
                    new File(jsonFilePath),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, ModelInfo.class)
            );
            currentId = modelInfos.get(0).getId();
            return modelInfos;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getCurrentId() {
        return currentId;
    }

}