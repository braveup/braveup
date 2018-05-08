package braveup.zk.example.basicexample;

import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * lei.song
 *
 * @author
 * @create 2018-05-07 下午5:50
 **/
public class DistributeCache {

    //公共资源
    private static List<String> msgCache = new ArrayList<String>();

    static class MsgConsumer extends Thread
    {
        @Override
        public void run()
        {
            while(!CollectionUtils.isEmpty(msgCache))
            {
                //先去获取锁
                String lock = ZookeeperLock.getInstance().getLock();
                if(CollectionUtils.isEmpty(msgCache))
                {
                    return;
                }
                String msg = msgCache.get(0);
                System.out.println(Thread.currentThread().getName() + " consume msg: " + msg);
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                msgCache.remove(msg);
                //释放锁，也就是将节点目录删除
                ZookeeperLock.getInstance().releaseLock(lock);
            }
        }
    }

    public static void main(String[] args)
    {
        for(int i = 0; i < 10; i++)
        {
            msgCache.add("msg" + i);
        }
        MsgConsumer consumer1 = new MsgConsumer();
        MsgConsumer consumer2 = new MsgConsumer();
        MsgConsumer consumer3 = new MsgConsumer();
        MsgConsumer consumer4 = new MsgConsumer();
        consumer1.start();
        consumer2.start();
        consumer3.start();
        consumer4.start();
    }
}