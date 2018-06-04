package com.gsafety.water;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gsafety.storm.SystemConfig;
import java.util.Properties;

/**
 * Created by Admin on 2017/11/7.
 */
public class IOTWaterTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConfig.get("KAFKA_BROKER_LIST"));
        //properties.put("group.id", "pxl-storm-water-test12");
        properties.put("group.id", "storm-kafka-water");
        //properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("consumer.topic", SystemConfig.get("KAFKA_TOPICS_WATER"));

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("IOTWaterSpout", new IOTWaterSpout(properties), 3);
        topologyBuilder.setBolt("IOTWaterBolt", new IOTWaterBolt(), 3).fieldsGrouping("IOTWaterSpout", new Fields("key"));

        StormTopology topology = topologyBuilder.createTopology();
        Config config = new Config();

        /*LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("IOWater", config, topology);*/

        config.setNumWorkers(3);
        StormSubmitter.submitTopology("IOTStream-water-test", config, topology);

    }
}
