package com.kafka.read;

import com.fasterxml.jackson.databind.JsonNode;
import org.json.JSONObject;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.neo4j.driver.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import static org.neo4j.driver.Values.parameters;

public class Neo4jBolt extends BaseBasicBolt {

    private Driver neo4jDriver;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // 初始化 Neo4j 驱动程序
        neo4jDriver = GraphDatabase.driver("bolt://CurriculumPractice:7687", AuthTokens.basic("neo4j", "12345678"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // 从 Tuple 中获取数据
        String modelId = tuple.getStringByField("modelId");
        String modelInfo = tuple.getStringByField("modelInfo");
        JSONObject modelInfoJson = new JSONObject(modelInfo);
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < modelInfoJson.getJSONArray("tags").length(); i++) {
            String tag = modelInfoJson.getJSONArray("tags").getString(i);
            tags.add(tag.replace("\"", "")); // 去掉每个标签的双引号
        }
        String author = modelInfoJson.getString("author");
        String downloads = modelInfoJson.getString("downloads");
        String pipeline_tag = modelInfoJson.getString("pipeline_tag");
        // 获取模型类型
//        String model_type = modelInfoJson.getJSONObject("config").getString("model_type");
        String model_type =  modelInfoJson.getString("library_name"); // 初始化为默认值
//        JSONObject configJson = modelInfoJson.optJSONObject("config");
//        if (configJson != null) {
//            // 处理 configJson
//            model_type = configJson.getString("model_type"); // 更新为 configJson 中的值
//        }
        String final_model_type=model_type;


        // 将数据存储到 Neo4j 图数据库中
        // 将数据存储到 Neo4j 图数据库中
        try (Session session = neo4jDriver.session()) {
            // 创建模型节点并设置属性
            session.writeTransaction(tx -> {
                Result result = tx.run(
                        "MERGE (m:Model {modelId: $modelId}) " +
                                "SET m.tags = $tags, m.modelTask = $pipeline_tag, m.modelFamily = $model_type, m.downloads=$downloads " +
                                "RETURN m",
                        parameters("modelId", modelId, "downloads",downloads,"tags", tags, "pipeline_tag", pipeline_tag,"model_type",final_model_type)
                );
                result.consume(); // 消耗查询结果
                return null;
            });

            // 创建模型作者节点并与模型节点建立关系
            String modelAuthor = getModelAuthor(author);
            if (!modelAuthor.isEmpty() && !modelAuthor.equals("null")) {
                System.out.println("modelAuthor:" + modelAuthor);
                session.writeTransaction(tx -> {
                    Result result = tx.run(
                            "MATCH (m:Model {modelId: $modelId}) " +
                                    "MERGE (author:ModelAuthor {name: $modelAuthor}) " +
                                    "MERGE (m)-[:BY]->(author)",
                            parameters("modelId", modelId, "modelAuthor", modelAuthor)
                    );
                    result.consume(); // 消耗查询结果
                    return null;
                });
            }

            // 创建模型家族节点并与模型节点建立关系
            String modelFamily = getModelFamily(model_type);
            if (!modelFamily.isEmpty() && !modelFamily.equals("null")) {
                System.out.println("modelFamily:" + modelFamily);
                session.writeTransaction(tx -> {
                    Result result = tx.run(
                            "MATCH (m:Model {modelId: $modelId}) " +
                                    "MERGE (family:ModelFamily {name: $modelFamily}) " +
                                    "MERGE (m)-[:BELONGS_TO]->(family)",
                            parameters("modelId", modelId, "modelFamily", modelFamily)
                    );
                    result.consume(); // 消耗查询结果
                    return null;
                });
            }

            // 创建任务节点并与模型节点建立关系
            String modelTask = getModelTask(pipeline_tag);
            if (!modelTask.isEmpty() && !modelTask.equals("null")) {
                System.out.println("modelTask:" + modelTask);
                session.writeTransaction(tx -> {
                    Result result = tx.run(
                            "MATCH (m:Model {modelId: $modelId}) " +
                                    "MERGE (task:ModelTask {name: $modelTask}) " +
                                    "MERGE (m)-[:PERFORMS]->(task)",
                            parameters("modelId", modelId, "modelTask", modelTask)
                    );
                    result.consume(); // 消耗查询结果
                    return null;
                });
            }

            // 创建数据集节点并与模型节点建立关系
            List<String> modelDatas = getModelData(tags);
            for (String dataset : modelDatas) {
                if (!dataset.isEmpty()) {
                    System.out.println("dataset:" + dataset);
                    session.writeTransaction(tx -> {
                        Result result = tx.run(
                                "MATCH (m:Model {modelId: $modelId}) " +
                                        "MERGE (dataSet:DataSet {name: $dataset}) " +
                                        "MERGE (m)-[:USES]->(dataSet)",
                                parameters("modelId", modelId, "dataset", dataset)
                        );
                        result.consume(); // 消耗查询结果
                        return null;
                    });
                }
            }
        }

    }
    private String getModelAuthor(String author) {
        // 由于model_type字段通常表示模型的任务或流水线标签，因此可以直接返回该字段的值
        return author != null ? author : "";
    }

    private String getModelFamily(String model_type) {
        // 由于model_type字段通常表示模型的任务或流水线标签，因此可以直接返回该字段的值
        return model_type != null ? model_type : "";
    }

    private String getModelTask(String pipeline_tag) {
        // 由于pipeline_tag字段通常表示模型的任务或流水线标签，因此可以直接返回该字段的值
        return pipeline_tag != null ? pipeline_tag : "";
    }

    private List<String> getModelData(List<String> tags) {
        // 创建一个列表来存储数据集
        List<String> datasets = new ArrayList<>();
        // 遍历标签数组，查找包含"dataset:"的标签
        for (String tag : tags) {
            // System.out.println("tag:" + tag);
            if (tag.startsWith("dataset:")) {
                // 提取出数据集并添加到列表中
                datasets.add(tag.substring("dataset:".length()));
            }
        }
        // 返回数据集列表
        return datasets;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 这个 Bolt 不发射任何数据，因此不需要声明任何字段
    }

    @Override
    public void cleanup() {
        // 关闭 Neo4j 驱动程序
        neo4jDriver.close();
    }
}
