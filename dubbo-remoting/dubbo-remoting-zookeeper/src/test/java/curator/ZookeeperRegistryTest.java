package curator;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.dubbo.common.URL;

/**
 * curator功能测试
 * 
 * @author onlyone
 */
public class ZookeeperRegistryTest {

    private String           zkPath = "172.16.110.91:2181";
    private CuratorFramework client;

    @Before
    public void setUp() throws Exception {
        try {
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkPath).retryPolicy(new RetryNTimes(
                                                                                                                                          Integer.MAX_VALUE,
                                                                                                                                          1000)).connectionTimeoutMs(5000);
            // String authority = url.getAuthority();
            // if (authority != null && authority.length() > 0) {
            // builder = builder.authorization("digest", authority.getBytes());
            // }
            client = builder.build();
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

                public void stateChanged(CuratorFramework client, ConnectionState state) {
                    if (state == ConnectionState.LOST) {
                        // CuratorZookeeperClient.this.stateChanged(StateListener.DISCONNECTED);
                    } else if (state == ConnectionState.CONNECTED) {
                        // CuratorZookeeperClient.this.stateChanged(StateListener.CONNECTED);
                    } else if (state == ConnectionState.RECONNECTED) {
                        // CuratorZookeeperClient.this.stateChanged(StateListener.RECONNECTED);
                    }
                }
            });
            client.start();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 是否建立连接
     */
    @Test
    public void testIsConnected() {
        boolean result = client.getZookeeperClient().isConnected();
        System.out.println(result);
    }

    /**
     * 创建节点，带内容（默认是持久节点）
     */
    @Test
    public void testCreatePersistent() {
        try {
            client.create().forPath("/register_boot", "123456".getBytes());
        } catch (NodeExistsException e) {
            System.out.println("/register_boot 节点已经存在");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 创建临时节点（当创建znode的客户端断开连接时,临时节点都会被删除）
     */
    @Test
    public void testCreateEphemeral() {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath("/register_boot/node8");
        } catch (NodeExistsException e) {
            System.out.println("sdfsfdsd");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 创建一个临时节点,并自动递归创建父节点
     */
    @Test
    public void testCreateEphemeral_1() {
        try {
            String leafNode = "dubbo://127.0.0.1:20880/com.xxx.XxxService?version=1.0.0&group=aa";

            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/register_boot2/"
                                                                                                     + URL.encode(leafNode));

        } catch (NodeExistsException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 删除一个节点
     */
    @Test
    public void testDelete() {
        try {
            // 删除一个节点（只能删除叶子节点）
            client.delete().forPath("/register_boot_1");

            // 删除一个节点，并递归删除其所有子节点
            // client.delete().deletingChildrenIfNeeded().forPath("/register_boot/node1");
            // 删除一个节点，强制指定版本进行删除
            // client.delete().withVersion(1111).forPath("");
            // 删除一个节点，强制保证删除
            // client.delete().guaranteed().forPath("");
        } catch (NoNodeException e) {
            System.out.println("1111");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    /**
     * 读取某个节点的值
     */
    @Test
    public void testGetData() {
        try {
            byte[] result = client.getData().forPath("/register_boot");
            System.out.println(new String(result));
        } catch (NoNodeException e) {
            System.out.println("testGetData  NoNodeException");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 更新某个节点的数据
     */
    @Test
    public void testSetData() {
        try {
            client.setData().forPath("/register_boot", "12345678".getBytes());
        } catch (NoNodeException e) {
            System.out.println("testGetData  NoNodeException");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 得到当前节点的子节点name
     */
    @Test
    public void testGetChildren() {
        try {
            List<String> list = client.getChildren().forPath("/register_boot2");
            System.out.println(list);
            for (String a : list) {
                System.out.println(URL.decode(a));
            }
        } catch (NoNodeException e) {
            System.out.println("NoNodeException");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 监听zk子节点变化情况
     */
    @Test
    public void testCuratorWatcher_1() {
        byte[] value = null;
        try {
            String path = "/register_boot";
            List<String> children = client.getChildren().usingWatcher(new CuratorWatcher() {

                @Override
                public void process(WatchedEvent event) throws Exception {
                    System.out.println("操作类型：" + event.getType());
                    if (event.getType() == EventType.NodeDataChanged) {
                        // 节点数据改变了，需要记录下来，以便session过期后，能够恢复到先前的数据状态
                        byte[] data = client.getData().usingWatcher(this).forPath(path);
                        // value = data;
                        System.out.println(path + ":" + new String(data));

                    } else if (event.getType() == EventType.NodeDeleted) {
                        // 节点被删除了，需要创建新的节点
                        System.out.println(path + ":" + path + " has been deleted.");
                        Stat stat = client.checkExists().usingWatcher(this).forPath(path);
                        if (stat == null) {
                            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path);
                        }

                    } else if (event.getType() == EventType.NodeCreated) {
                        // 节点被创建时，需要添加监听事件（创建可能是由于session过期后，curator的状态监听部分触发的）
                        System.out.println(path + ":" + " has been created!" + "the current data is "
                                           + new String(value));
                        client.setData().forPath(path, value);
                        client.getData().usingWatcher(this).forPath(path);

                    } else if (event.getType() == EventType.NodeChildrenChanged) {
                        List<String> currentChildren = client.getChildren().usingWatcher(this).forPath(path);
                        System.out.println("最新子节点：" + currentChildren);
                    }

                }
            }).forPath(path);

            System.out.println(children);
            Thread.sleep(1000000);
        } catch (NoNodeException e) {
            System.out.println("testPathChildrenCache NoNodeException");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    /**
     * 实现同testCuratorWatcher_1，内部采用递归形式，每次触发监听获取最新子节点，然后重新设置监听
     */
    @Test
    public void testCuratorWatcher_2() {
        try {
            String path = "/register_boot";
            List<String> children = client.getChildren().usingWatcher(new CuratorWatcherImpl()).forPath(path);

            System.out.println(children);
            Thread.sleep(1000000);
        } catch (NoNodeException e) {
            System.out.println("testPathChildrenCache NoNodeException");
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    private class CuratorWatcherImpl implements CuratorWatcher {

        public void process(WatchedEvent event) throws Exception {
            String path = event.getPath();
            List<String> children = client.getChildren().usingWatcher(this).forPath(event.getPath());
            System.out.println("path:" + path + ",children:" + children);
        }
    }

    /**
     * 关闭客户端
     */
    @Test
    public void doClose() {
        client.close();
    }

}
