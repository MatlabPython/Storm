package com.gsafety.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class IOTKafkaSpout implements IRichSpout {
  private SpoutOutputCollector spoutOutputCollector;
  private static final Logger LOG = LoggerFactory.getLogger(IOTKafkaSpout.class);
  private Properties properties;
  /**
   * 消费者
   */
  private transient KafkaConsumer<String, byte[]> consumer;

  public IOTKafkaSpout(Properties properties) {
    this.properties = properties;
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("key", "value", "topic","timestamp"));
  }

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }


  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.spoutOutputCollector = spoutOutputCollector;
    try {
      String[] topics = this.properties.getProperty("consumer.topic").split(",");
      this.consumer = new KafkaConsumer(properties);
      this.consumer.subscribe(Arrays.asList(topics));
      LOG.info("initialize KafkaConsumerSpout success");
    } catch (Exception e) {
      LOG.error("initialize KafkaConsumerSpout  faile", e);
    }
  }

  public void close() {
    if (this.consumer != null) {
      this.consumer.commitSync();
      this.consumer.close();
      LOG.info("KafkaConsumerSpout close!");
    }
  }

  public void activate() {

  }

  public void deactivate() {

  }

  /**
   * 发送反序列化后的数据
   */
  public void nextTuple() {
    ConsumerRecords<String, byte[]> records = consumer.poll(100L);
    for (ConsumerRecord<String, byte[]> record : records) {
      this.spoutOutputCollector.emit(new Values(record.key(), record.value(), record.topic(),new Date().getTime()));
    }
  }

  public void ack(Object o) {

  }

  public void fail(Object o) {

  }
}