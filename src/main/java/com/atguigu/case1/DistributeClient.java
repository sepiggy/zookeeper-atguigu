package com.atguigu.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 客户端监听/servers节点的目录变化
public class DistributeClient {

    //    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private String connectString = "127.0.0.1:52181,127.0.0.1:52182,127.0.0.1:52183";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        // 1 获取zk连接
        client.getConnect();

        // 2 监听/servers下面子节点的增加和删除
        client.getServerList();

        // 3 业务逻辑
        client.business();
    }

    private void business() throws InterruptedException {
        // 避免主线程退出
        Thread.sleep(Long.MAX_VALUE);
    }

    // 监听/servers节点目录变化
    private void getServerList() throws KeeperException, InterruptedException {

        List<String> children = zk.getChildren("/servers", true);

        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }

        // 打印
        System.out.println(servers);
    }

    // 获取ZK连接
    private void getConnect() throws IOException {

        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    // 保证时刻监听节点
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
