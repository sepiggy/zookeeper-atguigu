package com.atguigu.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

// 分布式锁的测试
public class DistributedLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        final DistributedLock lock1 = new DistributedLock();

        final DistributedLock lock2 = new DistributedLock();

        new Thread(() -> {
            try {
                lock1.lock();
                System.out.println("线程1获取到锁");
                Thread.sleep(5 * 1000);
                lock1.unlock();
                System.out.println("线程1释放锁");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.lock();
                System.out.println("线程2获取到锁");
                Thread.sleep(5 * 1000);
                lock2.unlock();
                System.out.println("线程2释放锁");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
