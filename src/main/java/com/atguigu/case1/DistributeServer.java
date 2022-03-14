package com.atguigu.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

// 服务器端将服务器信息注册到ZK
public class DistributeServer {

    //    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private String connectString = "127.0.0.1:52181,127.0.0.1:52182,127.0.0.1:52183";

    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();

        // 1 获取zk连接
        server.getConnect();

        // 2 注册服务器到zk集群
        server.register(args[0]);

        // 3 启动业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        // 避免主线程退出
        Thread.sleep(Long.MAX_VALUE);
    }

    private void register(String hostname) throws KeeperException, InterruptedException {
        // 创建临时顺序节点 -> /servers/hadoop102 /servers/hadoop103 /servers/hadoop104
        zk.create("/servers/" + hostname,
                hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + "已上线");
    }

    // 获取连接
    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
