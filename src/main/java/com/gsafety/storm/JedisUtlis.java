package com.gsafety.storm;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by Admin on 2018/1/23.
 */
public class JedisUtlis {

    public static void main(String[] args) {
        JedisCluster cluster = getJedisCluster();
        System.out.println(cluster.get("pxltest"));
    }


    public static JedisCluster getJedisCluster(){

        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        String[] redisPortArray = SystemConfig.get("REDIS_PORT").split(",");
        String  redisHost = SystemConfig.get("REDIS_HOST");
        for (int i = 0; i < redisPortArray.length; i++){
            nodes.add(new HostAndPort(redisHost, Integer.valueOf(redisPortArray[i])));

        }
        JedisCluster cluster =  new JedisCluster(nodes);

        return cluster;
    }
}
