package com.atguigu.case3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorLockTest {

    public static void main(String[] args) {

        CuratorFramework client = getCuratorFramework();

        // 启动客户端
        client.start();
        System.out.println("ZK启动成功");

        // 创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(client, "/locks");

        // 创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(client, "/locks");

        new Thread(() -> {
            try {
                lock1.acquire();
                System.out.println("线程1获取到锁");

                // 支持可重入
                lock1.acquire();
                System.out.println("线程1再次获取到锁");

                Thread.sleep(5 * 1000);

                lock1.release();
                System.out.println("线程1释放锁");

                lock1.release();
                System.out.println("线程1再次释放锁");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.acquire();
                System.out.println("线程2获取到锁");

                lock2.acquire();
                System.out.println("线程2再次获取到锁");

                Thread.sleep(5 * 1000);

                lock2.release();
                System.out.println("线程2释放锁");

                lock2.release();
                System.out.println("线程2再次释放锁");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static CuratorFramework getCuratorFramework() {

        // 连接失败重试策略
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:52181,127.0.0.1:52182,127.0.0.1:52183")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy).build();

        return client;
    }
}
