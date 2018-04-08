package com.gsafety.drain;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gsafety.gas.IOTGasSpout;
import com.gsafety.storm.SystemConfig;

import java.util.Properties;

/**
 * Author: huangll
 * Written on 17/8/29.
 */
public class IOTDrainTopology {

  public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", SystemConfig.get("KAFKA_BROKER_LIST"));
    properties.put("group.id", "storm-kafka-drain");
    //properties.put("auto.offset.reset", "earliest");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("consumer.topic", SystemConfig.get("KAFKA_TOPICS_DRAIN"));

    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("IOTDrainSpout", new IOTGasSpout(properties), 3);
    topologyBuilder.setBolt("IOTDrainBolt", new IOTDrainBolt(), 3).fieldsGrouping("IOTDrainSpout", new Fields("key"));

    StormTopology topology = topologyBuilder.createTopology();
    Config config = new Config();

   /* LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("IOTGasStream", config, topology);*/

    config.setNumWorkers(1);
    StormSubmitter.submitTopology("IOTStream-drain", config, topology);

  }
}

