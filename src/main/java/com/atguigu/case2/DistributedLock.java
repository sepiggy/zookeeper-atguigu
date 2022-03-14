package com.atguigu.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    //    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private String connectString = "127.0.0.1:52181,127.0.0.1:52182,127.0.0.1:52183";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zk;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath; // 监听前一个节点的路径
    private String currentNode; // 当前节点名

    public DistributedLock() throws IOException, InterruptedException, KeeperException {

        // 获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch  如果连接上zk  可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                // waitLatch  需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // 等待zk正常连接后，往下走程序
        connectLatch.await();

        // 判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);

        if (stat == null) {
            // 如果/locks不存在则创建一下根节点/locks
            zk.create("/locks", "locks".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
    }

    // 加锁
    public void lock() {
        try {
            // 创建对应的临时带序号节点
            currentNode = zk.create("/locks/" + "seq-",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);

            // wait一小会, 让结果更清晰一些
            Thread.sleep(10);

            /**
             * 判断创建的节点是否是最小的序号节点，
             * 如果是获取到锁；如果不是，监听它序号前一个节点
             */
            List<String> children = zk.getChildren("/locks", false);

            // 如果children 只有一个值，那就直接获取锁； 如果有多个节点，需要判断，谁最小
            if (children.size() == 1) {
                return;
            } else {
                // 对节点集合进行排序
                Collections.sort(children);

                // 获取节点名称, eg.seq-00000000
                String thisNode = currentNode.substring("/locks/".length());

                // 通过节点名称获取该节点在children集合的位置
                int index = children.indexOf(thisNode);

                // 判断
                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index == 0) {
                    // 就一个节点，直接获取锁
                    return;
                } else {
                    // 节点数 > 1, 需要监听它前一个节点变化
                    waitPath = "/locks/" + children.get(index - 1);

                    // 对前一个节点注册监听
                    // watch: true -> 调用获取连接时注册的监听方法
                    zk.getData(waitPath, true, new Stat());

                    // 等待监听
                    waitLatch.await();

                    return;
                }
            }


        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 解锁
    public void unlock() {

        // 删除节点
        try {
            zk.delete(this.currentNode, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
