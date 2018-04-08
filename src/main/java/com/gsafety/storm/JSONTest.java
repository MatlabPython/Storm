package com.gsafety.storm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class JSONTest {
  public static void main(String[] args) {
    HashMap<String, DynamicConfig> hashMap = new HashMap<>();

    DynamicConfig dynamicConfig = new DynamicConfig();
    dynamicConfig.setBurr(0.99);
    dynamicConfig.setZero(0.88);

    hashMap.put("0-1", dynamicConfig);

    DynamicConfig dynamicConfig2 = new DynamicConfig();
    dynamicConfig2.setBurr(4.33);
    dynamicConfig2.setZero(5.55);

    hashMap.put("0-2", dynamicConfig2);

    String s = JSON.toJSONString(hashMap);
    System.out.println(s);


    @SuppressWarnings("unchecked") HashMap<String, JSONObject> map = JSON.parseObject(s, HashMap.class);
    for (Map.Entry<String, JSONObject> entry : map.entrySet()) {
      System.out.println(entry.getKey());
      System.out.println(entry.getValue().getDouble("burr"));
      System.out.println(entry.getValue().getDouble("zero"));
    }

  }
}
