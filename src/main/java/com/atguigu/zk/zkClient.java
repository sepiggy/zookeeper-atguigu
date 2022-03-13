package com.atguigu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class zkClient {

    private ZooKeeper zkClient;

    // 注意：逗号左右不能有空格
    // private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private String connectString = "localhost:2181";

    private int sessionTimeout = 2000;

    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

//                System.out.println("-------------------------------");
//                List<String> children = null;
//                try {
//                    // 每次监听事件触发都手动再注册一次
//                    // 因为监听事件注册一次生效一次
//                    children = zkClient.getChildren("/", true);
//
//                    for (String child : children) {
//                        System.out.println(child);
//                    }
//
//                    System.out.println("-------------------------------");
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        });
    }

    // 通过客户端创建节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/girl",
                "dozy".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    // 通过客户端进行节点目录监听
    @Test
    public void getChildren() throws KeeperException, InterruptedException {

        // 使用初始化中注册的监听器
        List<String> children = zkClient.getChildren("/", true);

//        List<String> children = zkClient.getChildren("/", new Watcher() {
//
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//
//            }
//        });

        for (String child : children) {
            System.out.println(child);
        }

        // 保持运行
        Thread.sleep(Long.MAX_VALUE);
    }

    // 判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/girl", false);

        System.out.println(stat == null ? "not exist " : "exist");
    }
}
