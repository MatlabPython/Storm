package com.gsafety.bridgeQualityCk;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.gsafety.common.BaseZookeeper;
import com.gsafety.common.DBHelper;
import com.gsafety.lifeline.bigdata.avro.SensorData;
import com.gsafety.lifeline.bigdata.avro.SensorDataEntry;
import com.gsafety.lifeline.bigdata.util.AvroUtil;
import com.gsafety.storm.SystemConfig;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IOTBridgeQualityBolt extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(IOTBridgeQualityBolt.class);

    private Map<String, Long> preConfigMap = new HashMap<>();//公共支持设备状态用

    //kafka producer
    private KafkaProducer<String, byte[]> producer;

    private String nodeData;
//    public static Configuration configuration;
//    public static Connection connection;
//    public static Admin admin;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //zk连接
        BaseZookeeper baseZookeeper = new BaseZookeeper();
        try {
            baseZookeeper.connectZookeeper(SystemConfig.get("ZK_LIST"));
            nodeData = baseZookeeper.getData("/dynamic-config/ckquality/bridge");
            //kafka连接
            Properties proConf = new Properties();
            proConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SystemConfig.get("KAFKA_BROKER_LIST"));
            proConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            proConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producer = new KafkaProducer<>(proConf);
            //hbase的连接
//            configuration = HBaseConfiguration.create();
//            configuration.set("hbase.zookeeper.quorum", SystemConfig.get("ZK_LIST"));
//            try {
//                connection = ConnectionFactory.createConnection(configuration);
//                admin = connection.getAdmin();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
//        String sensorId = tuple.getStringByField("key");
        byte[] sensorDataBytes = (byte[]) tuple.getValueByField("value");
        SensorData sensorData = AvroUtil.deserialize(sensorDataBytes);
//        String topic = tuple.getStringByField("topic");
//        long timestamp = tuple.getLongByField("timestamp");
        String key = sensorData.getLocation().toString();
//        String key = "HF_PHDQ_00000001#2_8";
        String bridgeid = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("bridgeid").toString();
        String eqipmentname = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("equipmentname").toString();
        String monitoringcode = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("monitoringcode").toString();
        String range = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("range").toString();
        String simple_frequency = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("frequency").toString();
        List<SensorDataEntry> entries = sensorData.getEntries();
        /*
        数据监测表 BRIDGE_COMPLETE_INFO 完整表；BRIDGE_INTERUPT_INFO 中断表；BRIDGE_TIMELINESS_INFO 时效表；BRIDGE_NORM_INFO 规范表
         */

        /**
         * 数据的完整性
         */
        if (StringUtils.isNotEmpty(simple_frequency) && (1 / entries.size() != Integer.parseInt(simple_frequency) / 1000)) {
            logger.info("frequency------------------>" + Integer.parseInt(simple_frequency));
            //理论值：1000/frequency 实际值：entries.size() 桥梁名称：bridgename 监测项目：monitoringname 设备名称： terminal:sensorData.getTerminal().toString()
            //sensor:sensorData.getSensor() 采集时间：sensorData.getTime().toString()
            System.out.println("***********###############>>>>>>>" +
                    "桥梁名称->" + bridgeid +
                    "监测项目->" + monitoringcode +
                    "设备名称->" + eqipmentname +
                    "实际条数->" + entries.size() +
                    "理论条数->" + String.valueOf(1000 / Integer.parseInt(simple_frequency)) + "***********###############>>>>>>>");
            Statement statement = null;
            try {
                statement = DBHelper.createStatement();
                String CompeteSQL = "insert into BRIDGE_COMPLETE_INFO(bridgeid,monitoring_code,eqipment_name,actual_number,theory_number,time)" +
                        "VALUES ('" + bridgeid + "','" + monitoringcode + "','" + eqipmentname + "'," + entries.size() + "," + 1000 / Integer.parseInt(simple_frequency) + ",'" + sdf.format(sensorData.getTime()) + "')";
                logger.info(CompeteSQL);
                statement.execute(CompeteSQL);
                statement.close();
                DBHelper.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
//            try {
////                Table table = connection.getTable(TableName.valueOf("BRIDGE_COMPLETE_INFO"));
//                String startrowkey = MD5Hash.getMD5AsHex(bridgeid.getBytes("utf-8"));
//                String rowKey = startrowkey.substring(0, 6) + ":" + key + ":" + (Long.MAX_VALUE - sensorData.getTime());
//                Put put = new Put(Bytes.toBytes(rowKey));
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("monitoring_code"), Bytes.toBytes(monitoringcode));
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eqipment_name"), Bytes.toBytes(eqipmentname));
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("actual_number"), Bytes.toBytes(entries.size()));//实际数量
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("theory_number"), Bytes.toBytes(String.valueOf(1000 / frequency)));//理论数量
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(sensorData.getTime()));//理论数量
//                final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
//                    @Override
//                    public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
//                        for (int i = 0; i < e.getNumExceptions(); i++) {
//                            System.out.println("Failed to sent put " + e.getRow(i) + ".");
//                            logger.error("Failed to sent put " + e.getRow(i) + ".");
//                        }
//                    }
//                };
//                BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("BRIDGE_COMPLETE_INFO")).listener(listener);
//                params.writeBufferSize(5 * 1024 * 1024);
//                final BufferedMutator mutator = connection.getBufferedMutator(params);
//                try {
//                    mutator.mutate(put);
//                    mutator.flush();
//                } finally {
//                    mutator.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
        /**
         * 数据的规范性监测
         */
        if (StringUtils.isNotEmpty(range)) {
//            String range = JSONArray.fromObject(JSONObject.fromObject(nodeData).get(key)).getJSONObject(0).get("range").toString();
            Double max = Double.valueOf(range.substring(range.lastIndexOf(",") + 1, range.length()));//量程最大值
            Double min = Double.valueOf(range.substring(0, range.lastIndexOf(",")));//量程最小值
            int countRange = 0;
            for (int i = 0; i < entries.size(); i++) {
                SensorDataEntry sensorDataEntry = entries.get(i);
                List<Float> values = sensorDataEntry.getValues();
                if (values.get(0) > max || values.get(0) < min) {// 超出量程范围统计
                    countRange++;
                    System.out.println("***********###############>>>>>>>" +
                            "桥梁id->" + bridgeid +
                            "监测项目->" + monitoringcode +
                            "设备名称->" + eqipmentname +
                            "量程最大值->" + String.valueOf(max) +
                            "量程最小值->" + String.valueOf(min) +
                            "超量程值->" + values +
                            "数据时间->" + sdf.format(entries.get(i).getTime()) +
                            "***********###############>>>>>>>");
                    Statement statement = null;
                    try {
                        statement = DBHelper.createStatement();
                        String NormSQL = "insert into BRIDGE_NORM_INFO(bridgeid,monitoring_code,eqipment_name,out_range,max_range,min_range,time)" +
                                "VALUES ('" + bridgeid + "','" + monitoringcode + "','" + eqipmentname + "','" + values.get(0) + "'," + max + "," + min + ",'" + sdf.format(entries.get(i).getTime()) + "')";
                        logger.info(NormSQL);
                        statement.execute(NormSQL);
                        statement.close();
                        DBHelper.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    //                        try {
////                        Table table = connection.getTable(TableName.valueOf("BRIDGE_NORM_INFO"));
//                            String startrowkey = MD5Hash.getMD5AsHex(bridgeid.getBytes("utf-8"));
//                            String rowKey = startrowkey.substring(0, 6) + ":" + key + ":" + (Long.MAX_VALUE - entries.get(i).getTime());
//                            Put put = new Put(Bytes.toBytes(rowKey));
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("monitoring_code"), Bytes.toBytes(monitoringcode));
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eqipment_name"), Bytes.toBytes(eqipmentname));
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("out_range"), Bytes.toBytes(values.get(0).toString()));//超量程
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("max_range"), Bytes.toBytes(String.valueOf(max)));//量程最大值
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("min_range"), Bytes.toBytes(String.valueOf(min)));//量程最小值
//                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(entries.get(i).getTime().toString()));
//                            final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
//                                @Override
//                                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
//                                    for (int i = 0; i < e.getNumExceptions(); i++) {
//                                        System.out.println("Failed to sent put " + e.getRow(i) + ".");
//                                        logger.error("Failed to sent put " + e.getRow(i) + ".");
//                                    }
//                                }
//                            };
//                            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("BRIDGE_NORM_INFO")).listener(listener);
//                            params.writeBufferSize(5 * 1024 * 1024);
//                            final BufferedMutator mutator = connection.getBufferedMutator(params);
//                            try {
//                                mutator.mutate(put);
//                                mutator.flush();
//                            } finally {
//                                mutator.close();
//                            }
////                        table.put(put);
////                        table.close();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
                }
                /**
                 * 数据的时效性监测（接受数据时间-数据时间）
                 */
                long sysTime = sensorData.getTime();//接受时间
                long sedTime = entries.get(i).getTime();//数据时间
                if (sysTime - sedTime > 10 * 1000 || sysTime - sedTime < -5 * 1000) {
                    System.out.println("***********###############>>>>>>>" +
                            "桥梁id->" + bridgeid +
                            "监测项目->" + monitoringcode +
                            "设备名称->" + eqipmentname +
                            "数据时间->" + sdf.format(entries.get(i).getTime()) +
                            "接受时间->" + sdf.format(sensorData.getTime()) +
                            "值->" + values + "***********###############>>>>>>>");
                    Statement statement = null;
                    try {
                        statement = DBHelper.createStatement();
                        String TimeLinessSQL = "insert into BRIDGE_TIMELINESS_INFO(bridgeid,monitoring_code,eqipment_name,send_time,received_time,values)" +
                                "VALUES ('" + bridgeid + "','" + monitoringcode + "','" + eqipmentname + "','" + sdf.format(entries.get(i).getTime()) + "','" + sdf.format(sensorData.getTime()) + "','" + values + "')";
                        logger.info(TimeLinessSQL);
                        statement.execute(TimeLinessSQL);
                        statement.close();
                        DBHelper.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    /**
                     * HBASE的操作
                     */
//                    try {
////                    Table table = connection.getTable(TableName.valueOf("BRIDGE_TIMELINESS_INFO"));
//                        String startrowkey = MD5Hash.getMD5AsHex(bridgeid.getBytes("utf-8"));
//                        String rowKey = startrowkey.substring(0, 6) + ":" + key + ":" + (Long.MAX_VALUE - entries.get(i).getTime());
//                        Put put = new Put(Bytes.toBytes(rowKey));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("monitoring_code"), Bytes.toBytes(monitoringcode));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eqipment_name"), Bytes.toBytes(eqipmentname));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("send_time"), Bytes.toBytes(entries.get(i).getTime().toString()));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("receive_time"), Bytes.toBytes(sensorData.getTime().toString()));
//                        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
//                            @Override
//                            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
//                                for (int i = 0; i < e.getNumExceptions(); i++) {
//                                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
//                                    logger.error("Failed to sent put " + e.getRow(i) + ".");
//                                }
//                            }
//                        };
//                        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("BRIDGE_TIMELINESS_INFO")).listener(listener);
//                        params.writeBufferSize(5 * 1024 * 1024);
//                        final BufferedMutator mutator = connection.getBufferedMutator(params);
//                        try {
//                            mutator.mutate(put);
//                            mutator.flush();
//                        } finally {
//                            mutator.close();
//                        }
////                    table.put(put);
////                    table.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }
                /**
                 * 数据的中断性监测
                 */
                long prevDataTime = 0;
                long nextDataTime = 0;
                boolean flag = true;
                if (!preConfigMap.containsKey(key)) {
                    preConfigMap.put(key, entries.get(entries.size() - 1).getTime());//缓存上次数据包最后一条数据时间
                    flag = false;
                }
                nextDataTime = entries.get(0).getTime();
                if (flag && preConfigMap.containsKey(key) && nextDataTime - preConfigMap.get(key) > Integer.parseInt(simple_frequency)) {
                    prevDataTime = preConfigMap.get(key);
                    long inteDataTime = (nextDataTime - prevDataTime) / 1000;
                    System.out.println("***********###############>>>>>>>" +
                            "桥梁id->" + bridgeid +
                            "监测项目->" + monitoringcode +
                            "设备名称->" + eqipmentname +
                            "前一秒时间->" + sdf.format(prevDataTime) +
                            "下一秒时间->" + sdf.format(nextDataTime) +
                            "中断时长->" + String.valueOf((nextDataTime - prevDataTime) / 1000) +
                            "值->" + values + "***********###############>>>>>>>");
                    Statement statement = null;
                    try {
                        statement = DBHelper.createStatement();
                        String InteRuptSQL = "insert into BRIDGE_INTERUPT_INFO(bridgeid,monitoring_code,eqipment_name,prev_time,next_time,inte_time)" +
                                "VALUES ('" + bridgeid + "','" + monitoringcode + "','" + eqipmentname + "','" + sdf.format(prevDataTime) + "','" + sdf.format(nextDataTime) + "','" + String.valueOf((nextDataTime - prevDataTime) / 1000) + "')";
                        logger.info(InteRuptSQL);
                        statement.execute(InteRuptSQL);
                        statement.close();
                        DBHelper.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    //                    try {
////                    Table table = connection.getTable(TableName.valueOf("BRIDGE_INTERUPT_INFO"));
//                        String startrowkey = MD5Hash.getMD5AsHex(bridgeid.getBytes("utf-8"));
//                        String rowKey = startrowkey.substring(0, 6) + ":" + key + ":" + (Long.MAX_VALUE - entries.get(i).getTime());
//                        Put put = new Put(Bytes.toBytes(rowKey));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("monitoring_code"), Bytes.toBytes(monitoringcode));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eqipment_name"), Bytes.toBytes(eqipmentname));
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("prev_time"), Bytes.toBytes(prevDataTime));//上次时间
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("next_time"), Bytes.toBytes(nextDataTime));//下次时间
//                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("inte_time"), Bytes.toBytes(inteDataTime));//中断时间
//                        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
//                            @Override
//                            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
//                                for (int i = 0; i < e.getNumExceptions(); i++) {
//                                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
//                                    logger.error("Failed to sent put " + e.getRow(i) + ".");
//                                }
//                            }
//                        };
//                        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("BRIDGE_INTERUPT_INFO")).listener(listener);
//                        params.writeBufferSize(5 * 1024 * 1024);
//                        final BufferedMutator mutator = connection.getBufferedMutator(params);
//                        try {
//                            mutator.mutate(put);
//                            mutator.flush();
//                        } finally {
//                            mutator.close();
//                        }
////                    table.put(put);
////                    table.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }
                preConfigMap.put(key, entries.get(entries.size() - 1).getTime());
                //写入文件上传到hdfs上用kylin构建
            }
        }
        //写入文件上传到hdfs上用kylin构建
//        try {
//            connection.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static void main(String[] args) {
//        int AA = Integer.parseInt("");
//        System.out.println(AA);
//        Map<String, String> map = new ConcurrentHashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
//        Date date = new Date();
//        String time = sdf.format(date);
//        String endtime = "2018-06-01 08:31:00:000";
//        long stime = 0;
//        try {
//            stime = sdf.parse(endtime).getTime();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        System.out.println(stime);
        long testTime = 1528072822950l;
        String ss = sdf.format(testTime);
        System.out.println(ss);
    }
}
