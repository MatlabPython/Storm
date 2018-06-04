package com.gsafety.common;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author: yifeng G
 * @Date: Create in 12:33 2017/11/20 2017
 * @Description:
 * @Modified By:
 * @Vsersion:
 */

public class BaseZookeeper implements Watcher,Serializable {
    private ZooKeeper zookeeper;
    private static final int SESSION_TIME_OUT = 30000;
    public static final String ZK_HOST = "udap2:2181,udap3:2181,udap4:2181";
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            System.out.println("Watch received event");
            countDownLatch.countDown();
        }
    }

    /**
     * 连接zookeeper
     *
     * @param host
     * @throws Exception
     */
    public void connectZookeeper(String host) throws Exception {
        zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
        countDownLatch.await();
        System.out.println("zookeeper connection success");
    }

    /**
     * 创建节点
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public String createNode(String path, String data) throws Exception {
        return this.zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获取路径下所有子节点
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        List<String> children = zookeeper.getChildren(path, false);
        return children;
    }

    /**
     * 获取节点上面的数据
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zookeeper.getData(path, false, null);
        if (data == null) {
            return "";
        }
        return new String(data);
    }

    /**
     * 设置节点信息
     *
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat setData(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.setData(path, data.getBytes(), -1);
        return stat;
    }

    /**
     * 批量删除节点
     *
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteSubNode(String path) throws InterruptedException, KeeperException {
        //父节点
        if (zookeeper.getChildren(path, true).size() == 0) {
            System.out.println("Deleting Node Path >>>>>>>>> [" + path + " ]");
            zookeeper.delete(path, -1);
        } else {
            //递归查找非空子节点
            List<String> list = zookeeper.getChildren(path, true);
            for (String str : list) {
                zookeeper.delete(path + "/" + str, -1);
            }
        }
        closeConnection();
    }

    /**
     * 判断节点是否存在
     *
     * @param path
     * @throws Exception
     */
    public boolean existsNode(String path) throws Exception {
        boolean flag = false;//不存在
        Stat stat = zookeeper.exists(path, true);
        if (stat == null) {
            return flag;
        } else {
            flag = true;
        }
        return flag;//存在
    }

    /**
     * 获取创建时间
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getCTime(String path) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(path, false);
        return String.valueOf(stat.getCtime());
    }

    /**
     * 获取某个路径下孩子的数量
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Integer getChildrenNum(String path) throws KeeperException, InterruptedException {
        int childenNum = zookeeper.getChildren(path, false).size();
        return childenNum;
    }

    /**
     * 关闭连接
     *
     * @throws InterruptedException
     */
    public void closeConnection() throws InterruptedException {
        if (zookeeper != null) {
            zookeeper.close();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        BaseZookeeper baseZookeeper = new BaseZookeeper();
        try {
            baseZookeeper.connectZookeeper(ZK_HOST);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //获取"/" node下的所有子node
//        List<String> znodes = baseZookeeper.getChildren("/");
//        for (String path : znodes) {
//            System.out.println(path);
//        }
//        try {
//            baseZookeeper.createNode("/fixation-config/storm/bridge/"+"1_2", "");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        //创建开放权限的持久化node "/test"
//        String rs = zooKeeper.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode .PERSISTENT);
//        System.out.println(rs);
//
//        //同步获取"/test" node的数据
//        Stat stat = new Stat();
//        byte[] data = zooKeeper.getData("/test", true, stat);
//        System.out.println("value=" + new String(data));
//        System.out.println(stat.toString());
        //异步获取"/dubbo" node的数据
//        baseZookeeper.getData("/dubbo");
//        TimeUnit.SECONDS.sleep(10);
//        baseZookeeper.setData("/streamingDynamicConfig", "123");
        JSONObject ob = JSONObject.fromObject("{}");
        JSONArray jsonArray = JSONArray.fromObject("[]");
        String nodeData = baseZookeeper.getData("/dynamic-config/ckquality/bridge");
        ob = JSONArray.fromObject(JSONObject.fromObject(nodeData).get("HF_PHDQ_00000001#2_8")).getJSONObject(0);
        JSONObject.fromObject(nodeData).get("HF_PHDQ_00000001#2_8");
        System.out.println(Arrays.toString(new String[]{nodeData}));
//        System.out.println(nodeData);
//        baseZookeeper.deleteSubNode("/fixation-config/storm/bridge/1_2");
        baseZookeeper.closeConnection();
    }
}
