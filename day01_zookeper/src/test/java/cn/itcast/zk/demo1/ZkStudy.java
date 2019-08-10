package cn.itcast.zk.demo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.Test;

import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_REMOVED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_UPDATED;

public class ZkStudy {

    @Test
    public void creatNode() throws Exception{
        System.out.println("hello world");
        String connectString="node01:2181,node02:2181,node03:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
//获取客户端对象
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, 1000, 1000, retryPolicy);
//调用start开启客户端操作
        client.start();
        //通过create来进行创建节点，并且需要指定节点类型
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello3/world");
        client.close();

    }

    /**
     * 创建临时节点
     * @throws Exception
     */
    @Test
    public void createNode2() throws Exception {
        RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
        client.start();
        //Create.EPHEMERAL 临时节点  hello5是永久节点 world是临时节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello5/world");
        Thread.sleep(5000);
        client.close();
    }


    /**
     * 节点下面添加数据与修改是类似的，一个节点下面会有一个数据，新的数据会覆盖旧的数据
     * @throws Exception
     */
    @Test
    public void nodeData() throws Exception {
        RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
        client.start();
        client.setData().forPath("/hello5", "hello7".getBytes());
        client.close();
    }

    /**
     * 数据查询
     */
    @Test
    public void updateNode() throws Exception {
        RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
        client.start();
        byte[] forPath = client.getData().forPath("/hello5");
        System.out.println(new String(forPath));
        client.close();
    }

    /**
     * zookeeper的watch机制
     * @throws Exception
     */
    @Test
    public void watchNode() throws Exception {
        RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", policy);
        client.start();
        // ExecutorService pool = Executors.newCachedThreadPool();
        //设置节点的cache
        TreeCache treeCache = new TreeCache(client, "/hello5");
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if(data !=null){
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;

                        default:
                            break;
                    }
                }else{
                    System.out.println( "data is null : "+ event.getType());
                }
            }
        });
        //开始监听
        treeCache.start();
        Thread.sleep(50000000);
    }
}
