package com.gsafety.water;

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
 * Created by Admin on 2017/11/3.
 */
public class IOTWaterBolt extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(IOTWaterBolt.class);

    private Map<String, WaterFixation> configMap = new ConcurrentHashMap<>();
    private Map<String, String> statusConfigMap = new ConcurrentHashMap<>();//公共支持设备状态用
    private Map<String, List<ThresholdValueCommon>> handleConfigMap = new ConcurrentHashMap<>();

    private JedisCluster cluster;

    private Histogram histogram;

    private KafkaProducer<String, byte[]> producer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        cluster = JedisUtlis.getJedisCluster();
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

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, SystemConfig.get("ZK_DYNAMIC_CONFIG_WATER"), true);
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
                            WaterFixation waterConfig = waterConfigJsonParseBean(dataStr);
                            if (waterConfig != null) {
                                configMap.put(path, waterConfig);
                            }
                            List<ThresholdValueCommon> handleConfigList = getThresholdValueCommon(dataStr);
                            if (handleConfigList != null && handleConfigList.size() > 0) {
                                handleConfigMap.put(path, handleConfigList);
                            }
                            break;
                        }
                        case CHILD_UPDATED: {
                            String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                            byte[] data = event.getData().getData();
                            String dataStr = new String(data);
                            System.out.println("Node changed: " + path + " data " + dataStr);
                            WaterFixation waterConfig = waterConfigJsonParseBean(dataStr);
                            if (waterConfig != null) {
                                configMap.put(path, waterConfig);
                            }
                            List<ThresholdValueCommon> handleConfigList = getThresholdValueCommon(dataStr);
                            if (handleConfigList != null && handleConfigList.size() > 0) {
                                handleConfigMap.put(path, handleConfigList);
                            }
                            break;
                        }
                        case CHILD_REMOVED: {
                            String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                            byte[] data = event.getData().getData();
                            String dataStr = new String(data);
                            System.out.println("Node removed: " + path + " data " + dataStr);
                            configMap.remove(path);
                            handleConfigMap.remove(path);
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

        String status = statusConfigMap.get(sensorId);
        //拆包写回!!!无论什么状态
        takeWriteBackToKafka(sensorData, sensorId, topic);
        if (status == null) {//没有znode配置

            List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
            handleAlarm(sensorId, sensorDataEntries, sensorData);//阈值判断
            sendToRedis(sensorId, sensorDataEntries);
            int latency = (int) (new Date().getTime() - timestamp);
            histogram.update(latency);
            writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);//写回Kafka-pro

        } else {//znode配置了生效
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
            //其他状态不输出

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


    private Map<String, List<SensorDataEntry>> cacheSensorDataEntryList = new HashMap<>();//缓存数据

    //1毛刺处理
    private List<SensorDataEntry> handleBurr(String sensorId, SensorData sensorData) {
        WaterFixation waterConfig = configMap.get(sensorId);

        if (!cacheSensorDataEntryList.containsKey(sensorId)) {//如不过存在这个缓存一个
            List<SensorDataEntry> cacheSensorDataEntry = new ArrayList<SensorDataEntry>();
            cacheSensorDataEntryList.put(sensorId, cacheSensorDataEntry);
        }
        List<SensorDataEntry> que = cacheSensorDataEntryList.get(sensorId);//拿出空的或者什么的
        //WaterStatic  waterConfig = configMap.get(sensorId);//获得配置
        List<SensorDataEntry> entries = sensorData.getEntries();
        List<SensorDataEntry> mid = new ArrayList<>();
        List<SensorDataEntry> res = new ArrayList<>();
        for (SensorDataEntry entry : entries) {
            mid.add(entry);
        }
        if (waterConfig != null && waterConfig.getBurrMin() != null && waterConfig.getBurrMax() != null) {//如果有配置
            int sum1 = 0;//第一个持续五次报警计数
            int sum2 = 0;//第二个持续五次报警计数
            for (int i = 0; i < mid.size(); i++) {
                SensorDataEntry entry = mid.get(i);

                Double burrValue = Math.abs(waterConfig.getBurrMax());//△值
                Float sensorValue = mid.get(i).getValues().get(0);
                if (waterConfig.getSensorRangeMin() != null && waterConfig.getSensorRangeMax() != null) {
                    if (sensorValue > waterConfig.getSensorRangeMax() || sensorValue < waterConfig.getSensorRangeMin()) {//先判断是否超出量程
                        entry.setLevel(9);
                        this.writeToAlarmTopic(sensorId, entry, sensorData);
                        continue;
                    }
                }
                if (que.size() < 3) {
                    que.add(mid.get(i));
                    continue;
                } else {
                    Float lastValue = que.get(0).getValues().get(0);//X(i-1)
                    Float midValue = que.get(1).getValues().get(0);//X(i)
                    Float nextValue = que.get(2).getValues().get(0);//x(i+1)
                    /*if (mid.size() > 3){
                         lastValue = que.get(i - 3).getValues().get(0);//X(i-1)
                         midValue = que.get(i - 2).getValues().get(0);//X(i)
                         nextValue = que.get(i - 1).getValues().get(0);//x(i+1)
                    }else {}*/

                    Float y = midValue - lastValue;
                    Float nextY = nextValue - midValue;
                    if (Math.abs(y) <= burrValue) {//判断符合第一种
                        res.add(que.get(1));//如果符合取到符合值的下标存入返回数组中
                        que.add(mid.get(i));
                        que.remove(0);
                        sum1 = 0;
                        sum2 = 0;
                    } else if (Math.abs(nextValue - lastValue) <= burrValue) {//第二种情况
                        Float thisValue = (lastValue + nextValue) / 2;//修正后值
                        List<Float> thisList = new ArrayList<Float>();//取到集合
                        thisList.add(thisValue);
                        SensorDataEntry repairEntry = new SensorDataEntry();
                        repairEntry.setTime(entry.getTime());//修改值
                        repairEntry.setLevel(entry.getLevel());
                        repairEntry.setValues(thisList);
                        res.add(repairEntry);
                        que.add(mid.get(i));
                        que.remove(0);
                        sum1 = 0;
                        sum2 = 0;

                    } else if (Math.abs(nextY) <= burrValue) {//第三种情况 y+1
                        res.add(que.get(1));//如果符合取到符合值的下标存入返回数组中
                        sum1++;
                        if (sum1 >= 5) {//累计五次报警
                            writeToAlarmTopic(sensorId, mid.get(i - 2), sensorData);
                            sum1 = 0; //报警之后清零
                        }
                        sum2 = 0;
                        que.add(mid.get(i));
                        que.remove(0);
                    } else if (y * nextY >= 0) {//第四种情况
                        res.add(que.get(1));//如果符合取到符合值的下标存入返回数组中
                        sum2++;
                        if (sum2 >= 5) {//累计五次报警
                            writeToAlarmTopic(sensorId, mid.get(i - 2), sensorData);
                            sum2 = 0; //报警之后清零
                        }
                        sum1 = 0;
                        que.add(mid.get(i));
                        que.remove(0);
                   /* } else if(lastValue == midValue){
                        res.add(que.get(1));//如果符合取到符合值的下标存入返回数组中
                        sum1 = 0;
                        sum2 = 0;
                        que.add(mid.get(i));
                        que.remove(0);
                        System.out.println("第五种--"+midValue+"---无修正");*/
                    } else {
                        Float thisValue = lastValue;
                        List<Float> thisList = new ArrayList<Float>();//取到集合
                        thisList.add(thisValue);
                        SensorDataEntry repairEntry = new SensorDataEntry();
                        repairEntry.setTime(entry.getTime());//修改值
                        repairEntry.setLevel(entry.getLevel());
                        repairEntry.setValues(thisList);
                        res.add(repairEntry);
                        sum1 = 0;
                        sum2 = 0;
                        que.add(mid.get(i));
                        que.remove(0);

                    }
                }
            }

            return res;
        } else {//没有原样书输出
            return mid;
        }
    }

    private void handleAlarm(String sensorId, List<SensorDataEntry> sensorDataEntries, SensorData sensorData) {

        List<ThresholdValueCommon> waterAramList = handleConfigMap.get(sensorId);
        //检查配置如果配置了
        if (waterAramList != null && waterAramList.size() > 0) {
            for (SensorDataEntry sensorDataEntry : sensorDataEntries) {

                ThresholdValueCommon waterAramConfig = getRightConfig(waterAramList, sensorDataEntry);
                if (waterAramConfig == null) {
                    break;
                }
                Float value = sensorDataEntry.getValues().get(0);

                if (value > waterAramConfig.getAlarmFirstLevelUp() || value < waterAramConfig.getAlarmFirstLevelDown()) {
                    sensorDataEntry.setLevel(1);
                } else if ((value > waterAramConfig.getAlarmSecondLevelUp() && value < waterAramConfig.getAlarmFirstLevelUp()) ||
                        (value < waterAramConfig.getAlarmSecondLevelDown() && value > waterAramConfig.getAlarmFirstLevelDown())) {
                    sensorDataEntry.setLevel(2);
                } else if ((value > waterAramConfig.getAlarmThirdLevelUp() && value < waterAramConfig.getAlarmSecondLevelUp()) ||
                        (value < waterAramConfig.getAlarmThirdLevelDown() && value > waterAramConfig.getAlarmSecondLevelDown())) {
                    sensorDataEntry.setLevel(3);
                } else {
                    sensorDataEntry.setLevel(0);
                }

                if (sensorDataEntry.getLevel() > 0) {
                    writeToAlarmTopic(sensorId, sensorDataEntry, sensorData);
                }
            }
        }

    }


    //3存入redis
    private void sendToRedis(String sensorId, List<SensorDataEntry> sensorDataEntries) {
        if (cluster == null) {//不存在连接时候重新连
            cluster = JedisUtlis.getJedisCluster();
        }
        if (sensorDataEntries.size() > 0) {
            //滤过毛刺数据
            Map<String, Double> scoreMembers = new HashMap<>();
            for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
                //时间与服务器时间查过五分钟的数据不存入redis
                if (isBadSensorData(sensorDataEntry)) {
                    continue;
                }
                StringBuilder v = new StringBuilder();
                v.append(sensorDataEntry.getTime());
                List<Float> values = sensorDataEntry.getValues();
                v.append("_").append(values.get(0));
                //System.out.println("插入数据---"+sensorDataEntry.getValues().get(0));
                scoreMembers.put(v.toString(), sensorDataEntry.getTime() + 0.0);
            }
            //有有效数据，存入redis
            if (scoreMembers.size() > 0) {
                cluster.zadd(sensorId, scoreMembers);
                // zset 超过一千条， 删除旧数据
                Long count = cluster.zcard(sensorId);
                if (count > SystemConfig.getInt("ZSET_LENGTH")) {
                    cluster.zremrangeByRank(sensorId, 0, -1001);
                }
            }
        }
    }


    //5处理完数据写回kafka
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

    //写入报警topic
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
        //发送告警topic
        producer.send(new ProducerRecord<>(SystemConfig.get("WATER_ALARM_TOPIC"), sensorId, AvroUtil.serializer(sensorDetail)));
    }

    private ThresholdValueCommon getRightConfig(List<ThresholdValueCommon> configList, SensorDataEntry sensorDataEntry) {
        //在区间内
        long distance = sensorDataEntry.getTime() - TimeUtils.getTodayZeroTime();
        for (int i = 0; i < configList.size(); i++) {
            if (distance > configList.get(i).getStaTime() && distance < configList.get(i).getEndTime()) {
                return configList.get(i);
            }
        }
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private boolean isBadSensorData(SensorDataEntry sensorDataEntry) {
        return Math.abs(sensorDataEntry.getTime() - System.currentTimeMillis()) > SystemConfig.getInt("SENSOR_DATA_TIME_EXCEPTION");
    }

    /**
     * 传入字符串 转成bean
     *
     * @param json
     * @return
     */
    public WaterFixation waterConfigJsonParseBean(String json) {
        //由于取出字符串带有[] 其实就是有个集合 这么操作简单粗暴
        JSONObject jsonMid = JSONObject.parseObject(json);
        if (jsonMid != null && jsonMid.get("sensorRangeValue") != null) {
            return JSONObject.parseObject(jsonMid.get("sensorRangeValue").toString(), WaterFixation.class);
        } else if (jsonMid != null && jsonMid.get("status") != null) {
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

    public List<ThresholdValueCommon> getThresholdValueCommon(String json) {
        //由于取出字符串带有[] 其实就是有个集合 这么操作简单粗暴
        JSONObject waterJson = JSONObject.parseObject(json);//key  heat_fixation value arrayList
        if (waterJson != null && waterJson.get("statics") != null) {
            JSONArray jsonArray = JSONArray.parseArray(waterJson.get("statics").toString());
            List<ThresholdValueCommon> handleList = jsonArray.toJavaList(ThresholdValueCommon.class);
            return handleList;
        }

        return null;
    }
}
