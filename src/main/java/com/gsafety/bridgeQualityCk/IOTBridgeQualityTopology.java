package com.gsafety.bridgeQualityCk;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gsafety.storm.SystemConfig;
import java.util.Properties;

public class IOTBridgeQualityTopology {

  public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

    /**
     * kafka配置信息
     */
    Properties properties = new Properties();
    properties.put("bootstrap.servers", SystemConfig.get("KAFKA_BROKER_LIST"));
    properties.put("group.id", "storm-kafka-check");
    //properties.put("auto.offset.reset", "earliest");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("consumer.topic", SystemConfig.get("KAFKA_TOPICS_BRIDGE"));

    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("IOTBridgeQualitySpout", new IOTBridgeQualitySpout(properties), 3);
    topologyBuilder.setBolt("IOTBridgeQualityBolt", new IOTBridgeQualityBolt(), 3).fieldsGrouping("IOTBridgeQualitySpout", new Fields("key"));

    StormTopology topology = topologyBuilder.createTopology();
    Config config = new Config();
//
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("IOTStream-gas-test", config, topology);
//    config.setNumWorkers(1);
//    StormSubmitter.submitTopology("IOTStream-gas", config, topology);

  }
}

