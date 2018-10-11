package atguigu.server;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestZookeeper {

    private static String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init()throws Exception{

        zkClient = new ZooKeeper (connectString, sessionTimeout, event -> {});
    }

    @Test
    public void create () throws Exception{

        String nodeCreated = zkClient.create("/atguigu","jinlian".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void get() throws Exception{

        List<String> children = zkClient.getChildren("/", event -> {
            System.out.println(event.getType()+"--"+event.getPath());
            try {
                get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exit() throws Exception{

        Stat stat = zkClient.exists("/eclipse", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
