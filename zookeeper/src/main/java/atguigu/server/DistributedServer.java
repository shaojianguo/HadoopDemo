package atguigu.server;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributedServer {

    private static final String CONNECT_STRING = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private static final int SESSION_TIMEOUT = 2000;
    private static final String PARENT_NODE = "/servers";
    private ZooKeeper zkClient = null;
    {
        try {
            zkClient = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, event -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerServer(String hostname) throws Exception{
        zkClient.create(PARENT_NODE+"/server",hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void buziness() throws Exception{
        System.out.println("Server working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) {
        DistributedServer distributedServer = new DistributedServer();
        try {
            distributedServer.registerServer(args[0]);
            distributedServer.buziness();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
