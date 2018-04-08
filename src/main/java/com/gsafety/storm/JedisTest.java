package com.gsafety.storm;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Admin on 2018/1/23.
 */
public class JedisTest {
    public static void main(String[] args) {
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();

        nodes.add(new HostAndPort("10.5.4.3", 6380));
        nodes.add(new HostAndPort("10.5.4.3", 6381));
        nodes.add(new HostAndPort("10.5.4.3", 6382));
        nodes.add(new HostAndPort("10.5.4.3", 6383));
        nodes.add(new HostAndPort("10.5.4.3", 6384));
        nodes.add(new HostAndPort("10.5.4.3", 6385));
        JedisCluster cluster =  new JedisCluster(nodes);
        //cluster.set("pxltest","20180123");
        //cluster.get("GSD-[NDIR001710090261_0_0]")

        System.out.println(cluster.zrange("HSD-[HF_FHDD_DY000001_15_16]",0,-1));
    }
}
