package com.gsafety.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.codahale.metrics.Histogram;
import com.gsafety.lifeline.bigdata.avro.SensorData;
import com.gsafety.lifeline.bigdata.avro.SensorDataEntry;
import com.gsafety.lifeline.bigdata.avro.SensorDetail;
import com.gsafety.lifeline.bigdata.util.AvroUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Author: huangll
 * Written on 17/8/29.
 */
public class IOTProcessBolt extends BaseBasicBolt {

  private Logger logger = LoggerFactory.getLogger(IOTProcessBolt.class);

  private Map<String, DynamicConfigV3> configMap = new ConcurrentHashMap<>();

  private Jedis jedis;

  private Histogram histogram;

  //kafka producer
  private KafkaProducer<String, byte[]> producer;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    //初始化redis连接
    jedis = new Jedis(SystemConfig.get("REDIS_HOST"), SystemConfig.getInt("REDIS_PORT"));
    jedis.select(1);//桥1 水2  气3

    //初始化zk连接,拉取动态配置
    pullDynamicConfig();

    //初始化Kafka
    Properties proConf = new Properties();
    proConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SystemConfig.get("KAFKA_BROKER_LIST"));
    proConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    proConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producer = new KafkaProducer<>(proConf);

    this.histogram = LatencyMetrics.latencyHistogram();
  }

  private void pullDynamicConfig() {
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(SystemConfig.get("ZK_LIST"))
        .sessionTimeoutMs(5000)
        .connectionTimeoutMs(3000)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build();
    client.start();

    PathChildrenCache pathChildrenCache = new PathChildrenCache(client, SystemConfig.get("ZK_DYNAMIC_CONFIG"), true);
    try {
      pathChildrenCache.start();
      pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {

          switch (event.getType()) {
            case CHILD_ADDED: {
              String path = ZKPaths.getNodeFromPath(event.getData().getPath());
              byte[] data = event.getData().getData();
              String dataStr = new String(data);
              System.out.println("Node added: " + path + " data " + dataStr);
              DynamicConfigV3 DynamicConfigV3 = JSON.parseObject(dataStr, DynamicConfigV3.class);
              configMap.put(path, DynamicConfigV3);
              break;
            }
            case CHILD_UPDATED: {
              String path = ZKPaths.getNodeFromPath(event.getData().getPath());
              byte[] data = event.getData().getData();
              String dataStr = new String(data);
              System.out.println("Node changed: " + path + " data " + dataStr);
              DynamicConfigV3 DynamicConfigV3 = JSON.parseObject(dataStr, DynamicConfigV3.class);
              configMap.put(path, DynamicConfigV3);
              break;
            }
            case CHILD_REMOVED: {
              String path = ZKPaths.getNodeFromPath(event.getData().getPath());
              byte[] data = event.getData().getData();
              String dataStr = new String(data);
              System.out.println("Node removed: " + path + " data " + dataStr);
              configMap.remove(path);
              break;
            }
          }
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    String sensorId = tuple.getStringByField("key");
    byte[] sensorDataBytes = (byte[]) tuple.getValueByField("value");
    SensorData sensorData = AvroUtil.deserialize(sensorDataBytes);

    String topic = tuple.getStringByField("topic");
    long timestamp = tuple.getLongByField("timestamp");

    //拆包写回
    takeWriteBackToKafka(sensorData, sensorId, topic);

    //毛刺处理
    List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);

    //平衡清零
    sensorDataEntries = balenceToZero(sensorDataEntries, sensorId);

    //redis存最新一千条
    sendToRedis(sensorId, sensorDataEntries);

    int latency = (int) (new Date().getTime() - timestamp);
    histogram.update(latency);

    //阈值判断
    handleAlarm(sensorId, sensorDataEntries, sensorData);

    //写回Kafka
    writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);

  }
  //拆包写回kafka
  private void takeWriteBackToKafka(SensorData sensorData, String sensorId, String topic) {
    String location = sensorData.getLocation().toString();
    System.out.println(location+"-------------");
    String terminal = sensorData.getTerminal().toString();
    String sensor = sensorData.getSensor().toString();
    long rtime = sensorData.getTime();
    String sensorType = sensorData.getSensorType().toString();
    String dataType = sensorData.getDataType().toString();
    String monitoring = sensorData.getMonitoring().toString();
    List<SensorDataEntry> entries = sensorData.getEntries();
    for (int i = 0; i <entries.size(); i ++) {
      SensorDataEntry sensorDataEntry = entries.get(i);
      int level = sensorDataEntry.getLevel();
      long time = sensorDataEntry.getTime();
      List<Float> values = sensorDataEntry.getValues();
      SensorDetail sensorDetail = new SensorDetail();
      sensorDetail.setLocation(location);
      sensorDetail.setTerminal(terminal);
      sensorDetail.setSensor(sensor);
      sensorDetail.setRtime(rtime);
      sensorDetail.setSensorType(sensorType);
      sensorDetail.setDataType(dataType);
      sensorDetail.setMonitoring(monitoring);
      sensorDetail.setLevel(level);
      sensorDetail.setTime(time);
      sensorDetail.setValues(values);
      producer.send(new ProducerRecord<>(topic + "-DR", sensorId, AvroUtil.serializer(sensorDetail)));
    }

  }

  private void handleAlarm(String sensorId, List<SensorDataEntry> sensorDataEntries, SensorData sensorData) {
	
    DynamicConfigV3 dynamicConfig = configMap.get(sensorId);
	//检查配置如果配置了
    if (dynamicConfig != null && dynamicConfig.getAlarmConfigs() != null && dynamicConfig.getAlarmConfigs().size() > 0) {
      //阈值- 平衡清零值
      Double zreoValue = 0.0;
      if(dynamicConfig.getZero() != null){//如果配置了就减 没配置默认减0
        zreoValue = dynamicConfig.getZero();
      }
      for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
        AlarmConfig alarmConfig = getRightConfig(dynamicConfig, sensorDataEntry);
        if (alarmConfig == null) {
          break;
        }

        Float value = sensorDataEntry.getValues().get(0);
        if ((value > alarmConfig.getAlarmThirdLevelUp() - zreoValue && value < alarmConfig.getAlarmSecondLevelUp() - zreoValue) ||
            (value < alarmConfig.getAlarmThirdLevelDown() - zreoValue && value > alarmConfig.getAlarmSecondLevelDown() - zreoValue)) {
          sensorDataEntry.setLevel(3);
        } else if ((value > alarmConfig.getAlarmSecondLevelUp() - zreoValue && value < alarmConfig.getAlarmFirstLevelUp() - zreoValue) ||
            (value < alarmConfig.getAlarmSecondLevelDown() - zreoValue && value > alarmConfig.getAlarmFirstLevelDown() - zreoValue)) {
          sensorDataEntry.setLevel(2);
        } else if (value > alarmConfig.getAlarmFirstLevelUp() - zreoValue || value < alarmConfig.getAlarmFirstLevelDown() - zreoValue) {
          sensorDataEntry.setLevel(1);
        } else {
          sensorDataEntry.setLevel(0);
        }

        if (sensorDataEntry.getLevel() > 0) {
          writeToAlarmTopic(sensorId, sensorDataEntry, sensorData);
        }
      }
    }
  }

  private AlarmConfig getRightConfig(DynamicConfigV3 dynamicConfig, SensorDataEntry sensorDataEntry) {
    //判断是否有阈值配置
    if (dynamicConfig.getAlarmConfigs().get(0).getAlarmFirstLevelUp() == null) {
      return null;
    }

    if (dynamicConfig.getDynamic()) {
      return dynamicConfig.getAlarmConfigs().get(0);
    }

    long distance = sensorDataEntry.getTime() - TimeUtils.getTodayZeroTime();
    for (AlarmConfig alarmConfig : dynamicConfig.getAlarmConfigs()) {
      if (distance > alarmConfig.getStartTime() && distance < alarmConfig.getEndTime()) {
        return alarmConfig;
      }
    }

    return null;
  }

  private void writeToAlarmTopic(String sensorId, SensorDataEntry sensorDataEntry, SensorData sensorData) {
    List<Float> values = sensorDataEntry.getValues();
    SensorDetail sensorDetail = new SensorDetail();
    sensorDetail.setLocation(sensorData.getLocation());
    sensorDetail.setTerminal(sensorData.getTerminal());
    sensorDetail.setSensor(sensorData.getSensor());
    sensorDetail.setRtime(sensorData.getTime());
    sensorDetail.setSensorType(sensorData.getSensorType());
    sensorDetail.setDataType(sensorData.getDataType());
    sensorDetail.setMonitoring(sensorData.getMonitoring());
    sensorDetail.setLevel(sensorDataEntry.getLevel());
    sensorDetail.setTime(sensorDataEntry.getTime());
    sensorDetail.setValues(values);
    producer.send(new ProducerRecord<>(SystemConfig.get("BRIDGE_ALARM_TOPIC"), sensorId, AvroUtil.serializer(sensorDetail)));
  }


  private List<SensorDataEntry> balenceToZero(List<SensorDataEntry> sensorDataEntries, String sensorId) {
    if (configMap.get(sensorId) != null) {
      Double zero = configMap.get(sensorId).getZero();
      if (zero == null) {
        return sensorDataEntries;
      }
      for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
        if (sensorDataEntry.getValues().size() == 1) {
          List<Float> values = sensorDataEntry.getValues();
          Float res = values.get(0) - zero.floatValue();
          values.set(0, res);
        }
      }
    }
    return sensorDataEntries;
  }

  private Map<String, SensorDataEntry> cachedSensorData = new HashMap<>();
  private Map<String, Boolean> burrFlag = new HashMap<>();//TURE:正常,FALSE:毛刺

  private List<SensorDataEntry> handleBurr(String sensorId, SensorData sensorData) {
    List<SensorDataEntry> entries = sensorData.getEntries();
    ArrayList<SensorDataEntry> mid = new ArrayList<>();
    for (SensorDataEntry entry : entries) {
      mid.add((SensorDataEntry) entry);
    }

    if (!cachedSensorData.containsKey(sensorId)) {
      cachedSensorData.put(sensorId, mid.get(0));
    }

    ArrayList<SensorDataEntry> res = new ArrayList<>();

    for (SensorDataEntry entry : mid) {
      if (configMap.containsKey(sensorId) && configMap.get(sensorId).getBurrMax() != null && configMap.get(sensorId).getBurrMin() != null) {
        //只要在配置区间中,就是正常值
        if((entry.getValues().get(0) < configMap.get(sensorId).getBurrMax()) && ((entry.getValues().get(0)) > configMap.get(sensorId).getBurrMin())){
          res.add(entry);
          //内存缓存最新一条正常值
          cachedSensorData.put(sensorId, entry);
          continue;
        }

        float diff = entry.getValues().get(0) - cachedSensorData.get(sensorId).getValues().get(0);
        if (!burrFlag.containsKey(sensorId) || burrFlag.get(sensorId)) {
          //正常
          if (inTheRange(diff, configMap.get(sensorId))) {
            //这条不是毛刺
            burrFlag.put(sensorId, Boolean.TRUE);
            res.add(entry);
            //内存缓存最新一条正常值
            cachedSensorData.put(sensorId, entry);
          } else {
            burrFlag.put(sensorId, Boolean.FALSE);
            //该条是毛刺,输出上一条
            res.add(cachedSensorData.get(sensorId));
          }
        } else {
          res.add(entry);
          if (inTheRange(diff, configMap.get(sensorId))) {
            burrFlag.put(sensorId, Boolean.TRUE);
            //内存缓存最新一条正常值
            cachedSensorData.put(sensorId, entry);
          }
        }
      } else {
        //没有毛刺配置,原样返回
        res.add(entry);
      }

    }

    return res;
  }

  private boolean inTheRange(float diff, DynamicConfigV3 dynamicConfigV3) {
    return (diff < dynamicConfigV3.getBurrMax()) && (diff > dynamicConfigV3.getBurrMin());
  }

  private void sendToRedis(String sensorId, List<SensorDataEntry> sensorDataEntries) {
    if (sensorDataEntries.size() > 0) {
      // 过滤毛刺数据
      Map<String, Double> scoreMembers = new HashMap<>();
      for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
        //时间与服务器时间超过五分钟的数据不存入redis
        if (isBadSensorData(sensorDataEntry)) {
          continue;
        }
        StringBuilder v = new StringBuilder();
        v.append(sensorDataEntry.getTime());
        List<Float> values = sensorDataEntry.getValues();
        v.append("_").append(values.get(0));// 只取第一个
        scoreMembers.put(v.toString(), sensorDataEntry.getTime() + 0.0);
      }
      //有有效数据,存入redis
      if (scoreMembers.size() > 0) {
        jedis.zadd(sensorId, scoreMembers);

        // zset超过一千条，删除旧数据
        Long count = jedis.zcard(sensorId);
        if (count > SystemConfig.getInt("ZSET_LENGTH")) {
          jedis.zremrangeByRank(sensorId, 0, -1001);
        }
      }
    }
  }

  private boolean isBadSensorData(SensorDataEntry sensorDataEntry) {
    return Math.abs(sensorDataEntry.getTime() - System.currentTimeMillis()) > SystemConfig.getInt("SENSOR_DATA_TIME_EXCEPTION");
  }

  private void writeBackToKafka(SensorData sensorData, String topic, String sensorId, List<SensorDataEntry> sensorDataEntries) {
    String location = sensorData.getLocation().toString();
    System.out.println(location+"++++++++++++++++++++++++++++");
    String terminal = sensorData.getTerminal().toString();
    String sensor = sensorData.getSensor().toString();
    long rtime = sensorData.getTime();
    String sensorType = sensorData.getSensorType().toString();
    String dataType = sensorData.getDataType().toString();
    String monitoring = sensorData.getMonitoring().toString();


    for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
      int level = sensorDataEntry.getLevel();
      long time = sensorDataEntry.getTime();
      List<Float> values = sensorDataEntry.getValues();
      SensorDetail sensorDetail = new SensorDetail();
      sensorDetail.setLocation(location);
      sensorDetail.setTerminal(terminal);
      sensorDetail.setSensor(sensor);
      sensorDetail.setRtime(rtime);
      sensorDetail.setSensorType(sensorType);
      sensorDetail.setDataType(dataType);
      sensorDetail.setMonitoring(monitoring);
      sensorDetail.setLevel(level);
      sensorDetail.setTime(time);
      sensorDetail.setValues(values);
      producer.send(new ProducerRecord<>(topic + "-PRO", sensorId, AvroUtil.serializer(sensorDetail)));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
