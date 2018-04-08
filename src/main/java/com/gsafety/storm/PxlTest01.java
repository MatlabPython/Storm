package com.gsafety.storm;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Admin on 2017/11/7.
 */
public class PxlTest01 {
    public static void main(String[] args) {
        String dataStr ="{\"burrMax\":1.1499977111816406,\"burrMin\":-1.1499977111816406,\"dynamic\":true}";
        //String dataStr1 =""
        DynamicConfigV3 dynamicConfigV3 = JSON.parseObject(dataStr, DynamicConfigV3.class);
        System.out.println(dynamicConfigV3.getBurrMax());
        System.out.println(dynamicConfigV3.getZero());


    }
}
