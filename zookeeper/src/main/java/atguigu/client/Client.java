package atguigu.client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client {

    public static final String CONNECT_STRING = "hadoop102:2181,hadoop101:2181,hadoop103:2181";
    public static final int SESSION_TIMEOUT = 2000;
    public static final String PARENT_NODE = "/servers";
    private ZooKeeper zkClient = null;
    {
        try {
            zkClient = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, event -> {});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getServers(){
        try {
            List<String> children = zkClient.getChildren(PARENT_NODE, event -> getServers());

            ArrayList<String> servers = new ArrayList<>();

            for (String child : children) {
                byte[] data = zkClient.getData(PARENT_NODE+ "/"+ child,false,null);
                servers.add(new String(data));
            }

            System.out.println(servers);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void buziness(){
        System.out.println("Client working..");
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.getServers();
        client.buziness();
    }

}
