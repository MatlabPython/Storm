package com.gsafety.storm;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Admin on 2018/1/8.
 */
public class TestZnode {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("udap2:2181",5000, new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                System.out.println("监控文件中...");
            }
        });
        String znode = "/dynamic-config/heat";
        //String waterZnode = "/dynamic-config/heat";
        //桥梁------------------------
        /*List<String> list = zk.getChildren(znode,true);
        for (int i = 0;i < list.size();i ++ ){

            byte[] bytes = zk.getData(znode+"/"+list.get(i), true, null);
            String dataString = new String(bytes);
            *//*String sb = JSON.parseObject(dataString).get("gas_fixation").toString().replace("]","").replace("[","");
            GasFixation bd = JSON.parseObject(sb,GasFixation.class);*//*
            System.out.println(dataString);
        }*/

        List<String> heatList = zk.getChildren(znode,true);
        for (int j = 0;j < heatList.size();j ++ ){
            //System.out.println(heatList.get(j));
            byte[] bytes = zk.getData(znode+"/"+heatList.get(j), true, null);
            String dataString = new String(bytes);
            System.out.println(dataString);





            JSONObject jsonMid = JSONObject.parseObject(dataString);
            if (jsonMid != null && jsonMid.get("statics") != null) {
                JSONArray jsonArray = JSONArray.parseArray(jsonMid.get("statics").toString());
                List<ThresholdValueCommon> drainList = jsonArray.toJavaList(ThresholdValueCommon.class);
                System.out.println(drainList.size());
            } else if (jsonMid != null && jsonMid.get("status") != null) {
                JSONArray jsonArray = JSONArray.parseArray(jsonMid.get("status").toString());
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    Set<String> keyList = jsonObject.keySet();
                    for (String key : keyList) {
                        System.out.println("问题传感器sensorId---" + key);
                        System.out.println("问题传感器value---" + jsonObject.get(key).toString());
                    }
                }
            }


            //System.out.println(jsonMid);
           /* if(jsonMid != null  &&  jsonMid.get("bridge_dynamic") != null){
                BridgeDynamic  bd =JSONObject.parseObject(jsonMid.get("bridge_dynamic").toString(),BridgeDynamic.class);
                //System.out.println(bd.getAlarmFirstLevelDown());
            }else if (jsonMid != null && jsonMid.get("status") != null){
                JSONArray jsonArray = JSONArray.parseArray(jsonMid.get("status").toString());
                for(int i =0 ;i<jsonArray.size();i++){
                    JSONObject jsonObject =jsonArray.getJSONObject(i);
                    Set<String> it = jsonObject.keySet();
                    for(String key:it){
                        System.out.println(key);
                        System.out.println(jsonObject.get(key));
                    }

                }
            }*/

           //JSONObject json  =JSONObject.parseObject(dataString);
            /*JSONArray jsonArray = JSONArray.parseArray(json.get("heat_fixation").toString());
            List<HeatStatic> heatBeanList = jsonArray.toJavaList(HeatStatic.class);
            for(int k = 0; k < heatBeanList.size(); k ++){
                System.out.println(heatBeanList.get(k));
            }*/

            //String sb = JSON.parseObject(dataString);.get("bridge_fixation").toString().replace("]","").replace("[","");
            //BridgeDynamic bd = JSON.parseObject(sb,BridgeDynamic.class);
            //map =JSON.parseObject(dataString);

        }
      /*  byte[] bytes = zk.getData(znode+"/test", true, null);
        String dataString = new String(bytes);
        JSONObject json  =JSONObject.parseObject(dataString);
        JSONArray jsonArray = JSONArray.parseArray(json.get("water_static").toString());
        List<ThresholdValueCommon> list = jsonArray.toJavaList(ThresholdValueCommon.class);
        for(int k = 0; k < list.size(); k ++){
            System.out.println(list.get(k).getAlarmFirstLevelDown());
        }*/
    }
}
