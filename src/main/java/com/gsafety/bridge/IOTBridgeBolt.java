package com.gsafety.bridge;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
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
import redis.clients.jedis.JedisCluster;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Author: huangll
 * Written on 17/8/29.
 */
public class IOTBridgeBolt extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(IOTBridgeBolt.class);

    private Map<String, String> statusConfigMap = new ConcurrentHashMap<>();//公共支持设备状态用

    private Map<String, BridgeDynamic> configMap = new ConcurrentHashMap<>();

    private JedisCluster cluster;

    private Histogram histogram;

    //kafka producer
    private KafkaProducer<String, byte[]> producer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        cluster = JedisUtlis.getJedisCluster();//初始化redis连接
        //jedis = new Jedis(SystemConfig.get("REDIS_HOST"), SystemConfig.getInt("REDIS_PORT"));
        //jedis.select(SystemConfig.getInt("BRIDGE_REDIS_NUM"));//桥1 水2 气3
        //jedis.select(0);//桥1 水2  气3
        pullDynamicConfig(); //初始化zk连接,拉取动态配置
        Properties proConf = new Properties();//初始化Kafka
        proConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SystemConfig.get("KAFKA_BROKER_LIST"));
        proConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        proConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(proConf);
        this.histogram = LatencyMetrics.latencyHistogram();
    }

    //监听znode节点的状态
    private void pullDynamicConfig() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(SystemConfig.get("ZK_LIST"))
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, SystemConfig.get("ZK_DYNAMIC_CONFIG_BRIDGE"), true);
        try {
            pathChildrenCache.start();
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {

                    switch (event.getType()) {
                        case CHILD_ADDED: {
                            String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                            byte[] data = event.getData().getData();
                            if (data != null) {
                                String dataStr = new String(data);
                                System.out.println("Node added: " + path + " data " + dataStr);
                                BridgeDynamic bridgeConfig = bridgeConfigJsonParseBean(dataStr);
                                if (bridgeConfig != null) {
                                    configMap.put(path, bridgeConfig);
                                }
                            }
                            break;
                        }
                        case CHILD_UPDATED: {
                            String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                            byte[] data = event.getData().getData();
                            if (data != null) {
                                String dataStr = new String(data);
                                System.out.println("Node changed: " + path + " data " + dataStr);
                                BridgeDynamic bridgeConfig = bridgeConfigJsonParseBean(dataStr);
                                if (bridgeConfig != null) {
                                    configMap.put(path, bridgeConfig);
                                }
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
        String status = statusConfigMap.get(sensorId);
        takeWriteBackToKafka(sensorData, sensorId, topic);//拆包写回-Dr
        if (status == null) {//没有设备状态znode配置

            if ("PV".equals(sensorData.getDataType().toString())) {//PV类型
                List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
                sensorDataEntries = balenceToZero(sensorDataEntries, sensorId);//平衡清零
                sendToRedis(sensorId, sensorDataEntries);//redis存最新一千条
                int latency = (int) (new Date().getTime() - timestamp);
                histogram.update(latency);
                handleAlarm(sensorId, sensorDataEntries, sensorData);//阈值判断
                writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);//写回Kafka
            } else if ("S10M".equals(sensorData.getDataType().toString())) {//s10M类型
                SensorData s10mSensorData = balenceToZeroS10M(sensorData, sensorId);
                writeBackToKafka(s10mSensorData, topic, sensorId, sensorData.getEntries());
            } else {//其他类型
                writeBackToKafka(sensorData, topic, sensorId, sensorData.getEntries());
            }
        } else {//有znode配置

            if ("3".equals(status)) {//正常状态 全部输出
                if ("PV".equals(sensorData.getDataType().toString())) {//PV类型
                    List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
                    sensorDataEntries = balenceToZero(sensorDataEntries, sensorId);//平衡清零
                    sendToRedis(sensorId, sensorDataEntries);//redis存最新一千条
                    int latency = (int) (new Date().getTime() - timestamp);
                    histogram.update(latency);
                    handleAlarm(sensorId, sensorDataEntries, sensorData);//阈值判断
                    writeBackToKafka(sensorData, topic, sensorId, sensorDataEntries);//写回Kafka
                } else if ("S10M".equals(sensorData.getDataType().toString())) {//s10M类型
                    SensorData s10mSensorData = balenceToZeroS10M(sensorData, sensorId);
                    writeBackToKafka(s10mSensorData, topic, sensorId, sensorData.getEntries());
                } else {//其他类型
                    writeBackToKafka(sensorData, topic, sensorId, sensorData.getEntries());
                }
            }
            if ("2".equals(status)) {//调试只写入redis

                if ("PV".equals(sensorData.getDataType().toString())) {//PV类型
                    List<SensorDataEntry> sensorDataEntries = handleBurr(sensorId, sensorData);//毛刺处理
                    sensorDataEntries = balenceToZero(sensorDataEntries, sensorId);//平衡清零
                    sendToRedis(sensorId, sensorDataEntries);//redis存最新一千条
                }
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

        BridgeDynamic bridgeConfig = configMap.get(sensorId);
        if (bridgeConfig != null) { //如果有配置
            //阈值- 平衡清零值
            Double zreoValue = 0.0;
            if (bridgeConfig.getZero() != null) {//如果配置了就减 没配置默认减0
                zreoValue = bridgeConfig.getZero();
            }
            for (SensorDataEntry sensorDataEntry : sensorDataEntries) {

                if (bridgeConfig == null) {
                    break;
                }

                Float value = sensorDataEntry.getValues().get(0);
                if (value > bridgeConfig.getAlarmFirstLevelUp() - zreoValue || value < bridgeConfig.getAlarmFirstLevelDown() - zreoValue) {
                    sensorDataEntry.setLevel(1);
                } else if ((value > bridgeConfig.getAlarmSecondLevelUp() - zreoValue && value < bridgeConfig.getAlarmFirstLevelUp() - zreoValue) ||
                        (value < bridgeConfig.getAlarmSecondLevelDown() - zreoValue && value > bridgeConfig.getAlarmFirstLevelDown() - zreoValue)) {
                    sensorDataEntry.setLevel(2);
                } else if ((value > bridgeConfig.getAlarmThirdLevelUp() - zreoValue && value < bridgeConfig.getAlarmSecondLevelUp() - zreoValue) ||
                        (value < bridgeConfig.getAlarmThirdLevelDown() - zreoValue && value > bridgeConfig.getAlarmSecondLevelDown() - zreoValue)) {
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


    private Map<String, Long> cacheSensorTime = new HashMap<String, Long>();

    private void writeToAlarmTopic(String sensorId, SensorDataEntry sensorDataEntry, SensorData sensorData) {

        //BridgeDynamic bridgeConfig = configMap.get(sensorId);
        Long cacheTime = 10000L;//报警间隔

      /*if(bridgeConfig != null && bridgeConfig.getAlarmInterval() != 0){
          cacheTime = bridgeConfig.getAlarmInterval();
      }*/

        if (!cacheSensorTime.containsKey(sensorId)) {//如不过存在这个缓存一个 先发一条记录发送时间


            cacheSensorTime.put(sensorId, sensorDataEntry.getTime() + cacheTime);//把此条报警信息时间加上缓存起来
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
        } else {
            Long sendTime = cacheSensorTime.get(sensorId);//取出进行比较
            if (sensorData.getTime() - sendTime > 0) {//判断时间是否大于频率 大于才发送
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
                cacheSensorTime.put(sensorId, sensorDataEntry.getTime() + cacheTime);//更新此条报警时间加上缓存起来 下次进行比较
            }
        }
    }


    private List<SensorDataEntry> balenceToZero(List<SensorDataEntry> sensorDataEntries, String sensorId) {

        if (configMap.get(sensorId) != null) {
            Double zero = configMap.get(sensorId).getZero();
            if (zero == null) {
                return sensorDataEntries;
            }
            for (SensorDataEntry sensorDataEntry : sensorDataEntries) {
                List<Float> values = sensorDataEntry.getValues();
                Float res = values.get(0) - zero.floatValue();
                values.set(0, res);
            }
        }
        return sensorDataEntries;
    }

    /**
     * s10M平衡清零值
     *
     * @param sensorData
     * @param sensorId
     * @return
     */
    private SensorData balenceToZeroS10M(SensorData sensorData, String sensorId) {
        if (configMap.get(sensorId) != null) {
            Double zero = configMap.get(sensorId).getZero();
            if (zero == null) {
                return sensorData;
            }
            List<Float> values = sensorData.getEntries().get(0).getValues();
            Float resOne = values.get(0) - zero.floatValue();
            Float resTwo = values.get(1) - zero.floatValue();
            Float resThr = values.get(2) - zero.floatValue();
            values.set(0, resOne);
            values.set(1, resTwo);
            values.set(2, resThr);
        }
        return sensorData;
    }

    private Map<String, Long> cacheHandleBurrTime = new HashMap<String, Long>();
    private Map<String, List<SensorDataEntry>> cachedSensorData = new HashMap<>();
    private Map<String, Boolean> burrFlag = new HashMap<>();//TURE:正常,FALSE:毛刺

    private List<SensorDataEntry> handleBurr(String sensorId, SensorData sensorData) {
        List<SensorDataEntry> entries = sensorData.getEntries();
        ArrayList<SensorDataEntry> mid = new ArrayList<>();
        for (SensorDataEntry entry : entries) {
            mid.add(entry);
        }
        ArrayList<SensorDataEntry> res = new ArrayList<>();
        for (SensorDataEntry entry : mid) {
            if (burrFlag.get(sensorId) == null || burrFlag.get(sensorId) == false) { //判断前面数据是否有疑似毛刺数据 flag =true是有
                if (configMap.containsKey(sensorId) && configMap.get(sensorId).getBurrMax() != null && configMap.get(sensorId).getBurrMin() != null) {//配置判断
                    //只要在配置区间中,就是正常值
                    if ((entry.getValues().get(0) < configMap.get(sensorId).getBurrMax()) && ((entry.getValues().get(0)) > configMap.get(sensorId).getBurrMin())) {//判断是否在毛刺区间
                        res.add(entry);//正常输出用
                    } else {
                        burrFlag.put(sensorId, true);//true代表有毛刺
                        cacheHandleBurrTime.put(sensorId, entry.getTime() + 2000);//缓存时间观察2秒
                        List<SensorDataEntry> burrSensorList = new ArrayList<>();//开始缓存加载第一条
                        burrSensorList.add(entry);
                        cachedSensorData.put(sensorId, burrSensorList);
                        //System.out.println("此条数据疑似毛刺开始注意值sensorid"+sensorId+"--毛刺缓存开始时间"+entry.getTime()+"毛刺缓存结束时间"+entry.getTime() + 2000+"--时间--"+parseTimeToDate(entry.getTime())+"--"+entry.toString());
                        continue;
                    }
                } else {
                    //没有毛刺配置,原样返回
                    res.add(entry);
                }

            } else {//有毛刺开始缓存  //1先判断时间 大于当前时间是缓存够了 2小于当然时间继续缓存  3 完成之后清空缓存
                if (cachedSensorData.get(sensorId) != null && cachedSensorData.get(sensorId).size() > 0) {
                    Long burrTime = cacheHandleBurrTime.get(sensorId);//大于当前时间才证明缓存够了
                    if (entry.getTime() > burrTime) { //大于当前时间缓存够了

                        List<SensorDataEntry> list = cachedSensorData.get(sensorId);
                        //System.out.println("此处发生毛刺缓存缓存id--"+sensorId+"---大小"+list.size() +"数据时间"+entry.getTime()+"毛刺时间"+burrTime);
                        if ((list.get(list.size() - 1).getValues().get(0) < configMap.get(sensorId).getBurrMax()) && ((entry.getValues().get(0)) > configMap.get(sensorId).getBurrMin())) {//判断是否在毛刺区间 1s后数据正常的说明上疑似是真的需要修正、反之说明为正常数据
                            List<SensorDataEntry> repairList = RepairData(list, sensorId);//修正数据
                            for (int i = 0; i < repairList.size(); i++) {
                                res.add(repairList.get(i));//输出其他的
                            }
                            //System.out.println("编号"+sensorId+"传感器毛刺疑似实锤开始修正");
                            //System.out.println("观察数据最后一条数据"+list.get(list.size()-1).getValues().get(0)+"毛刺上区间"+configMap.get(sensorId).getBurrMax()+"毛刺下区间"+configMap.get(sensorId).getBurrMin()+"--时间--"+parseTimeToDate(entry.getTime())+"--"+entry.toString());
                        } else {
                            for (int i = 0; i < list.size(); i++) {
                                res.add(list.get(i));//正常输出用所有的
                            }
                            //System.out.println("编号"+sensorId+"传感器毛刺疑似解除");
                            //System.out.println("观察数据最后一条数据"+list.get(list.size()-1).getValues().get(0)+"毛刺上区间"+configMap.get(sensorId).getBurrMax()+"毛刺下区间"+configMap.get(sensorId).getBurrMin()+"--时间--"+parseTimeToDate(entry.getTime())+"--"+entry.toString());
                        }

                        burrFlag.put(sensorId, false);// 回到最初的状态
                    } else {//小于当前时间证明没缓存够继续缓存

                        List<SensorDataEntry> list = cachedSensorData.get(sensorId);
                        list.add(entry);
                        cachedSensorData.put(sensorId, list);
                        //System.out.println("继续缓存当前sensorId"+sensorId+"数组大小"+list.size());
                        continue;
                    }
                }
            }

        }
        return res;
    }

    private boolean inTheRange(float diff, DynamicConfigV3 dynamicConfigV3) {
        return (diff < dynamicConfigV3.getBurrMax()) && (diff > dynamicConfigV3.getBurrMin());
    }

    private void sendToRedis(String sensorId, List<SensorDataEntry> sensorDataEntries) {
        if (cluster == null) {//不存在连接时候，重新连
            cluster = JedisUtlis.getJedisCluster();
        }
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
            //有效数据,存入redis
            if (scoreMembers.size() > 0) {
                cluster.zadd(sensorId, scoreMembers);
                // zset超过一千条，删除旧数据
                Long count = cluster.zcard(sensorId);
                if (count > SystemConfig.getInt("ZSET_LENGTH")) {
                    cluster.zremrangeByRank(sensorId, 0, -1001);
                }
            }
        }
    }

    private boolean isBadSensorData(SensorDataEntry sensorDataEntry) {
        return Math.abs(sensorDataEntry.getTime() - System.currentTimeMillis()) > SystemConfig.getInt("SENSOR_DATA_TIME_EXCEPTION");
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
        if (dynamicConfigV3.getZero() != null && dynamicConfigV3.getBurrMin() != null && dynamicConfigV3.getBurrMin() != null
                && dynamicConfigV3.getAlarmConfigs() != null && dynamicConfigV3.getAlarmConfigs().size() != 0
                && dynamicConfigV3.getAlarmConfigs().size() < 2 && dynamicConfigV3.getDynamic() != null) {
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

    public List<SensorDataEntry> RepairData(List<SensorDataEntry> sensorDataEntrys, String sensorId) {
        Double burrMax = configMap.get(sensorId).getBurrMax();
        Double burrMin = configMap.get(sensorId).getBurrMin();
        Float value = (float) ((burrMax + burrMin) / 2);
        for (int i = 0; i < sensorDataEntrys.size(); i++) {
            SensorDataEntry entry = sensorDataEntrys.get(i);
            if ((entry.getValues().get(0) < burrMax) && (entry.getValues().get(0) > burrMin)) {
                //缓存一个值用于修补数据
                value = entry.getValues().get(0);
                //System.out.println("修补值-"+value);
                continue;
            } else {//毛刺数据
                List<Float> values = entry.getValues();
                //System.out.println("毛刺上区间"+burrMax+"毛刺下区间"+burrMin+"修正前---" + entry.getValues().get(0) +"---修正后—--"+value+"-传感器编号-"+sensorId+"--时间--"+parseTimeToDate(entry.getTime())+"--"+entry.toString());
                values.set(0, value);
            }
        }

        return sensorDataEntrys;

    }

    private String parseTimeToDate(Long hm) {
        Date date = new Date(hm);
        SimpleDateFormat format = new SimpleDateFormat("yyyy年MM月dd日 hh:mm:ss SSS a");
        return format.format(date);
    }

    public BridgeDynamic bridgeConfigJsonParseBean(String json) {
        //由于取出字符串带有[] 其实就是有个集合 这么操作简单粗暴

        JSONObject jsonMid = JSONObject.parseObject(json);
        if (jsonMid != null && jsonMid.get("bridge_dynamic") != null) {
            return JSONObject.parseObject(jsonMid.get("bridge_dynamic").toString(), BridgeDynamic.class);
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


}
