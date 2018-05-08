package braveup.zk.code;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


/**
 * lei.song
 *
 * @author
 * @create 2018-05-07 下午5:10
 **/
public class ZookeeperLock {
    private static Logger logger = Logger.getLogger(ZookeeperLock.class);

    private String ROOT_LOCK_PATH = "Locks";
    private String RPE_LOCK_NAME = "mylock_";
    private static ZookeeperLock zookeeperLock;
    private String zookeeperIp = "";
    private static ZooKeeper zkClient = null;

    /**
     * 获取分布式锁实例
     *
     * @return
     */
    public static ZookeeperLock getInstance() {
        if (null == zookeeperLock) {
            zookeeperLock = new ZookeeperLock();
        }
        return zookeeperLock;
    }


    /**
     * 获取锁，即创建子节点，当该节点成为序号最小的节点时则获取锁
     */
    public String getLock() {
        // 关键方法，创建包含自增长id名称的目录，这个方法支持了分布式锁的实现
        // 四个参数：
        // 1、目录名称
        // 2、目录文本信息
        // 3、文件夹权限，Ids.OPEN_ACL_UNSAFE表示所有权限
        // 4、目录类型，CreateMode.EPHEMERAL_SEQUENTIAL表示会在目录名称后面加一个自增加数字
        String lockPath = null;
        try {
            // 创建EPHEMERAL_SEQUENTIAL类型节点
            lockPath = getZkClient().create(
                    ROOT_LOCK_PATH + '/' + RPE_LOCK_NAME,
                    Thread.currentThread().getName().getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + " create lock path : " + lockPath);
            // 尝试获取锁
            tryLock(lockPath);
        } catch (Exception e) {
            logger.error("发生未知异常-->", e);
        }
        return lockPath;
    }

    /**
     * 该函数是一个递归函数 如果获得锁，直接返回；否则，阻塞线程，等待上一个节点释放锁的消息，然后重新tryLock
     */
    private boolean tryLock(String lockPath) throws InterruptedException, KeeperException {
        // 获取ROOT_LOCK_PATH下所有的子节点，并按照节点序号排序
        List<String> lockPaths = getZkClient().getChildren(ROOT_LOCK_PATH, false);
        Collections.sort(lockPaths);
        int index = lockPaths.indexOf(lockPath.substring(ROOT_LOCK_PATH.length() + 1));
        if (index == 0) {// lockPath是序号最小的节点，则获取锁
            System.out.println(Thread.currentThread().getName() + " get lock, lock path: " + lockPath);
            return true;
        } else {
            // 创建Watcher，监控lockPath的前一个节点
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 创建的锁目录只有删除事件
                    System.out.println("Received delete event, node path is " + event.getPath());
                    synchronized (this) {
                        notifyAll();
                    }
                }
            };

            String preLockPath = lockPaths.get(index - 1);
            // 查询前一个目录是否存在，并且注册目录事件监听器，监听一次事件后即删除
            Stat state = null;
            try {
                state = getZkClient().exists(ROOT_LOCK_PATH + "/" + preLockPath, watcher);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 返回值为目录详细信息
            // 由于某种原因，前一个节点不存在了（比如连接断开），重新tryLock
            if (state == null) {
                return tryLock(lockPath);
            } else {
                // 阻塞当前进程，直到preLockPath释放锁，重新tryLock
                System.out.println(Thread.currentThread().getName() + " wait for " + preLockPath);
                synchronized (watcher) {
                    // 等待目录删除事件唤醒
                    watcher.wait();
                }
                return tryLock(lockPath);
            }
        }
    }


    /**
     * 释放锁：实际上是删除当前线程目录
     *
     * @param lockPath
     */
    public void releaseLock(String lockPath) {
        try {
            getZkClient().delete(lockPath, -1);
            System.out.println("Release lock, lock path is" + lockPath);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }


    /**
     * 连接客户端
     *
     * @return
     */
    public ZooKeeper getZkClient() {
        if (null == zkClient)
            try {
                zkClient = new ZooKeeper(zookeeperIp, 3000, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.printf("**watcher receive WatchedEvent** changed path: " + event.getPath()
                                + "; changed type: " + event.getType().name());
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        return zkClient;
    }
}

