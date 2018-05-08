package braveup.zk.example.distributequeue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * lei.song
 * <p>
 * 分布式队列
 *
 * @author
 * @create 2018-05-08 下午2:01
 **/
public class ZkDistributedQueue {
    //场景
    /**
     * 很多单机上很平常的事情，放在集群环境中都会发生质的变化。
     * <p>
     * 以一个常见的生产者-消费者模型举例：有一个容量有限的邮筒，寄信者（即生产者）不断地将信件塞入邮筒，邮递员（即消费者）不断地从邮筒取出信件发往目的地。运行期间需要保证：
     * <p>
     * （1）邮筒已达上限时，寄信者停止活动，等带邮筒恢复到非满状态
     * <p>
     * （2）邮筒已空时，邮递员停止活动，等带邮筒恢复到非空状态
     * <p>
     * 该邮筒用有序队列实现，保证FIFO（先进先出）特性。
     * <p>
     * 在一台机器上，可以用有序队列来实现邮筒，保证FIFO（先进先出）特性，开启两个线程，一个充当寄信者，一个充当邮递员，通过wait()/notify()很容易实现上述功能。
     * <p>
     * 但是，如果在跨进程或者分布式环境下呢？比如，一台机器运行生产者程序，另一台机器运行消费者程序，代表邮筒的有序队列无法跨机器共享，但是两者需要随时了解邮筒的状态（是否已满、是否已空）以及保证信件的有序（先到达的先发送）。
     * <p>
     * 这种情况下，可以借助ZooKeeper实现一个分布式队列。新建一个“/mailBox”节点代表邮筒。一旦有信件到达，就在该节点下创建PERSISTENT_SEQUENTIAL类型的子节点，当子节点总数达到上限时，阻塞生产者，然后使用getChildren(String path, Watcher watcher)方法监控子节点的变化，子节点总数减少后再回复生产；而消费者每次选取序号最小的子节点进行处理，然后删除该节点，当子节点总数为0时，阻塞消费者，同样设置监控，子节点总数增加后再回复消费。
     */
    //邮箱上限为10
    private static final int MAILBOX_MAX_SIZE = 10;

    // 邮箱路径
    private static final String MAILBOX_ROOT_PATH = "/mailBox";

    // 信件节点
    private static final String LETTER_NODE_NAME = "letter_";

    //生产者线程，用于接受信件
    static class Producer extends Thread {

        ZooKeeper zkclient;

        @Override
        public void run() {
            while (true) {
                try {
                    //信箱已满
                    if (getLetterNum() == MAILBOX_MAX_SIZE) {
                        System.out.println("mailBox has been full");
                        //创建watcher监控子节点变化
                        Watcher watcher = new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                // 生产者已停止，只有消费者在活动，所以只可能出现发送信件的动作
                                System.out.println("mailBox has been not full");
                                synchronized (this) {
                                    notify(); // 唤醒生产者
                                }
                            }
                        };

                        zkclient.getChildren(MAILBOX_ROOT_PATH, watcher);

                        synchronized (this) {
                            watcher.wait();
                        }
                    } else {
                        // 线程随机休眠数毫秒，模拟现实中的费时操作
                        int sleepMillis = (int) (Math.random() * 1000);
                        Thread.sleep(sleepMillis);

                        // 接收信件，创建新的子节点
                        String newLetterPath = zkclient.create(MAILBOX_ROOT_PATH + "/" + LETTER_NODE_NAME,
                                "letter".getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT_SEQUENTIAL);
                        System.out.println("a new letter has been received: "
                                + newLetterPath.substring(MAILBOX_ROOT_PATH.length() + 1)
                                + ", letter num: " + getLetterNum());
                    }
                } catch (Exception e) {
                    System.out.println("producer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }

        }

        private int getLetterNum() throws KeeperException, InterruptedException {
            Stat stat = zkclient.exists(MAILBOX_ROOT_PATH, null);
            int numChildren = stat.getNumChildren();
            return numChildren;
        }

        public void setZkClient(ZooKeeper zkclient) {
            this.zkclient = zkclient;
        }
    }


    // 消费者线程，负责发送信件
    static class Consumer extends Thread {

        ZooKeeper zkclient;

        @Override
        public void run() {

            while (true) {
                try {
                    if (getLetterNum() == 0) {//
                        System.out.println("mailBox has been empty");
                        Watcher watcher = new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                // 消费者已停止，只有生产者在活动，所以只可能出现收取信件的动作
                                System.out.println("mailBox has been not empty");
                                synchronized (this) {
                                    notify();// 唤醒消费者
                                }
                            }
                        };

                        zkclient.getChildren(MAILBOX_ROOT_PATH, watcher);
                        synchronized (this) {
                            watcher.wait();
                        }
                    } else {
                        // 线程随机休眠数毫秒，模拟现实中的费时操作
                        int sleepMillis = (int) Math.random() * 1000;
                        Thread.sleep(sleepMillis);

                        // 发送信件，删除序号最小的子节点
                        String firstLetter = getFirstLetter();
                        zkclient.delete(MAILBOX_ROOT_PATH + "/" + firstLetter, -1);
                        System.out.println("a letter has been delivered: " + firstLetter
                                + ", letter num: " + getLetterNum());

                    }
                } catch (Exception e) {
                    System.out.println("Consumer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }
        }

        private String getFirstLetter() throws KeeperException, InterruptedException {
            List<String> lockPaths = zkclient.getChildren(MAILBOX_ROOT_PATH, false);
            Collections.sort(lockPaths);
            return lockPaths.get(0);
        }

        private int getLetterNum() throws KeeperException, InterruptedException {
            Stat stat = zkclient.exists(MAILBOX_ROOT_PATH, null);
            int numChildren = stat.getNumChildren();
            return numChildren;
        }


        public void setZkclient(ZooKeeper zkclient) {
            this.zkclient = zkclient;
        }
    }

    public static void main(String[] args) throws IOException {
        // 开启生产者线程
        Producer producer = new Producer();
        ZooKeeper zkClientA = new ZooKeeper("127.0.0.1:2181", 3000, null);
        producer.setZkClient(zkClientA);
        producer.start();

        // 开启消费者线程
        Consumer consumer = new Consumer();
        ZooKeeper zkClientB = new ZooKeeper("127.0.0.1:2181", 3000, null);
        consumer.setZkclient(zkClientB);
        consumer.start();
    }

}
