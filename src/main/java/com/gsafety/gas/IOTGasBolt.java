package com.gsafety.gas;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Histogram;
import com.gsafety.lifeline.bigdata.avro.SensorData;
import com.gsafety.lifeline.bigdata.avro.SensorDataEntry;
import com.gsafety.lifeline.bigdata.avro.SensorDetail;
import com.gsafety.lifeline.bigdata.util.AvroUtil;
import com.gsafety.storm.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Author: huangll
 * Written on 17/8/29.
 */
public class IOTGasBolt extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(IOTGasBolt.class);

    private Map<String, String> statusConfigMap = new ConcurrentHashMap<>();//公共支持设备状态用
    private Map<String, GasFixation> configMap = new ConcurrentHashMap<>();

    private JedisCluster cluster;
    private Jedis jedis;
    private Histogram histogram;

    //kafka producer
    private KafkaProducer<String, byte[]> producer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //初始化redis连接
        cluster = JedisUtlis.getJedisCluster();
        jedis = new Jedis("10.5.4.41", 6379);
        jedis.select(3);//桥1 水2  气3

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

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, SystemConfig.get("ZK_DYNAMIC_CONFIG_GAS"), true);
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
                            GasFixation gasConfig = gasConfigJsonParseBean(dataStr);
                            if (gasConfig != null) {
                                configMap.put(path, gasConfig);
                            }
                            break;
                        }
                        case CHILD_UPDATED: {
                            String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                            byte[] data = event.getData().getData();
                            String dataStr = new String(data);
                            System.out.println("Node changed: " + path + " data " + dataStr);
                            GasFixation gasConfig = gasConfigJsonParseBean(dataStr);
                            if (gasConfig != null) {
                                configMap.put(path, gasConfig);
                            }
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
        takeWriteBackToKafka(sensorData, sensorId, topic);//拆包写回无论什么状态
        String status = statusConfigMap.get(sensorId);
        if (status == null) {//没有znode配置

            List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
            sendToRedis(sensorId, sensorDataEntries);//redis存最新一千条
            int latency = (int) (new Date().getTime() - timestamp);
            histogram.update(latency);
            handleAlarm(sensorId, sensorDataEntries, sensorData);//阈值判断
            writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);//写回Kafka-pro
        }else{
            if ("3".equals(status)) {//正常 全部输出

                List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
                handleAlarm(sensorId, sensorDataEntries, sensorData); //阈值判断
                sendToRedis(sensorId, sensorDataEntries);
                int latency = (int) (new Date().getTime() - timestamp);
                histogram.update(latency);
                writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);//写回Kafka-pro
            }

            if ("2".equals(status)) {//调试 只写入redis
                List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
                handleAlarm(sensorId, sensorDataEntries, sensorData);//阈值判断
                sendToRedis(sensorId, sensorDataEntries);
            }
        }


    }

    //拆包写回kafka
    private void takeWriteBackToKafka(SensorData sensorData, String sensorId, String topic) {

        String location = sensorData.getLocation().toString();
        String terminal = sensorData.getTerminal().toString();
        String sensor = sensorData.getSensor().toString();
        long rtime = sensorData.getTime();
        String sensorType = sensorData.getSensorType().toString();
        String dataType = sensorData.getDataType().toString();
        String monitoring = sensorData.getMonitoring().toString();
        List<SensorDataEntry> entries = sensorData.getEntries();
        for (int i = 0; i < entries.size(); i++) {
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
        GasFixation gasConfig = configMap.get(sensorId);
        //检查配置如果配置了
        for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
            if (gasConfig == null) {
                break;
            }
            Float value = sensorDataEntry.getValues().get(0);

            if (value < gasConfig.getAlarmFirstLevelUp() && value > gasConfig.getAlarmFirstLevelDown()) {
                sensorDataEntry.setLevel(1);
            } else if ((value < gasConfig.getAlarmSecondLevelUp() && value > gasConfig.getAlarmSecondLevelDown())) {
                sensorDataEntry.setLevel(2);
            } else if (value < gasConfig.getAlarmThirdLevelUp() && value > gasConfig.getAlarmThirdLevelDown()) {
                sensorDataEntry.setLevel(3);
            } else {
                sensorDataEntry.setLevel(0);
            }

            if (sensorDataEntry.getLevel() > 0) {
                writeToAlarmTopic(sensorId, sensorDataEntry, sensorData);
            }
        }
    }

    private AlarmConfig getRightConfig(DynamicConfigV3 dynamicConfig, SensorDataEntry sensorDataEntry) {
        //判断是否有阈值配置
        if (dynamicConfig.getAlarmConfigs().get(0).getAlarmFirstLevelUp() == null) {
            return null;
        }
    /*if (dynamicConfig.getDynamic()) {
      return dynamicConfig.getAlarmConfigs().get(0);
    }*/

   /* long distance = sensorDataEntry.getTime() - TimeUtils.getTodayZeroTime();
    for (AlarmConfig alarmConfig : dynamicConfig.getAlarmConfigs()) {
      if (distance > alarmConfig.getStartTime() && distance < alarmConfig.getEndTime()) {
        return alarmConfig;
      }
    }*/

        return dynamicConfig.getAlarmConfigs().get(0);
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

        producer.send(new ProducerRecord<>(SystemConfig.get("GAS_ALARM_TOPIC"), sensorId, AvroUtil.serializer(sensorDetail)));
    }


    private List<SensorDataEntry> handleBurr(String sensorId, SensorData sensorData) {
        List<SensorDataEntry> entries = sensorData.getEntries();
        ArrayList<SensorDataEntry> mid = new ArrayList<>();
        for (SensorDataEntry entry : entries) {
            mid.add(entry);
        }

        return mid;
    }


    private void sendToRedis(String sensorId, List<SensorDataEntry> sensorDataEntries) {
        if (cluster == null) {//不存在连接时候重新连
            cluster = JedisUtlis.getJedisCluster();
        }
        if(jedis == null){
            jedis = new Jedis("10.5.4.41", 6379);
            jedis.select(3);//桥1 水2  气3
        }
        if (sensorDataEntries.size() > 0) {
            // 过滤毛刺数据
            Map<String, Double> scoreMembers = new HashMap<>();
            for (SensorDataEntry sensorDataEntry : sensorDataEntries) {

        /*if (isBadSensorData(sensorDataEntry)) {
          continue;
        }*/
                StringBuilder v = new StringBuilder();
                v.append(sensorDataEntry.getTime());
                List<Float> values = sensorDataEntry.getValues();
                for (int i = 0; i < values.size(); i++) {
                    v.append("_").append(values.get(i));
                }
                scoreMembers.put(v.toString(), sensorDataEntry.getTime() + 0.0);
            }
            //有有效数据,存入redis
            if (scoreMembers.size() > 0) {
                cluster.zadd(sensorId, scoreMembers);
                jedis.zadd(sensorId, scoreMembers);

                // zset超过一千条，删除旧数据
                Long count = cluster.zcard(sensorId);
                if (count > SystemConfig.getInt("ZSET_LENGTH")) {
                    cluster.zremrangeByRank(sensorId, 0, -1001);
                }

                Long count1 = jedis.zcard(sensorId);
                if (count1 > SystemConfig.getInt("ZSET_LENGTH")){
                    jedis.zremrangeByRank(sensorId, 0, -1001);
                }

            }
        }
    }

    private boolean isBadSensorData(SensorDataEntry sensorDataEntry) {
        return sensorDataEntry.getTime() - System.currentTimeMillis() > 3000000;
        //return (sensorDataEntry.getTime() - System.currentTimeMillis()) > 0 ;
    }

    private void writeBackToKafka(SensorData sensorData, String topic, String sensorId, List<SensorDataEntry> sensorDataEntries) {
        String location = sensorData.getLocation().toString();
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

    public DynamicConfigV3 checkZnodeDynamicCondigV3(DynamicConfigV3 dynamicConfigV3) {
        if (dynamicConfigV3.getAlarmConfigs() != null && dynamicConfigV3.getAlarmConfigs().size() != 0) {
            AlarmConfig alarmConfig = dynamicConfigV3.getAlarmConfigs().get(0);
            if (alarmConfig.getAlarmFirstLevelDown() != null && alarmConfig.getAlarmFirstLevelUp() != null &&
                    alarmConfig.getAlarmSecondLevelDown() != null && alarmConfig.getAlarmSecondLevelUp() != null &&
                    alarmConfig.getAlarmThirdLevelDown() != null && alarmConfig.getAlarmThirdLevelUp() != null) {

                return dynamicConfigV3;

            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * 传入字符串 转成bean
     *
     * @param json
     * @return
     */
    public GasFixation gasConfigJsonParseBean(String json) {
        //由于取出字符串带有[] 其实就是有个集合 这么操作简单粗暴
        JSONObject jsonMid = JSONObject.parseObject(json);
        if (jsonMid != null && jsonMid.get("gas_fixation") != null) {
            return JSONObject.parseObject(jsonMid.get("gas_fixation").toString(), GasFixation.class);
        }else if (jsonMid != null && jsonMid.get("status") != null) {
            JSONArray jsonArray = JSONArray.parseArray(jsonMid.get("status").toString());
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                Set<String> keyList = jsonObject.keySet();
                for (String key : keyList) {
                    statusConfigMap.put(key, jsonObject.get(key).toString());
                    System.out.println("问题传感器sensorId---" + key);
                    System.out.println("问题传感器value---" + jsonObject.get(key).toString());
                }
            }
        }

        return null;
    }
}
