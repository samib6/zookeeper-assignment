import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Executor implements Watcher {

    ZooKeeper zk;

    public Executor(String hostPort) throws Exception {
        // Connect to ZooKeeper server
        zk = new ZooKeeper(hostPort, 3000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        // We do not use watches in this part of the assignment
        System.out.println("[WATCHER] Event received: " + event);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("Usage: java Executor host:port");
            return;
        }

        String hostPort = args[0];

        Executor executor = new Executor(hostPort);

        // Small delay to ensure ZooKeeper client connects
        Thread.sleep(500);

        // --------------------------------------
        // 1. CREATE /watch_test1
        // --------------------------------------
        System.out.println("Creating /watch_test1 ...");

        executor.zk.create(
                "/watch_test1",
                "initial_value".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        System.out.println("[CREATE] rc=" + rc + ", path=" + path + ", name=" + name);

                        // After creation → set data
                        setData(executor);
                    }
                },
                null
        );

        // Keep the program alive long enough for async callbacks
        Thread.sleep(5000);
    }

    // --------------------------------------
    // 2. SET DATA
    // --------------------------------------
    static void setData(Executor executor) {
        System.out.println("Setting data for /watch_test1 ...");

        executor.zk.setData(
                "/watch_test1",
                "updated_value".getBytes(),
                -1,
                new AsyncCallback.StatCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                        System.out.println("[SET DATA] rc=" + rc + ", path=" + path);

                        // After setting data → get data
                        getData(executor);
                    }
                },
                null
        );
    }

    // --------------------------------------
    // 3. GET DATA
    // --------------------------------------
    static void getData(Executor executor) {
        System.out.println("Getting data for /watch_test1 ...");

        executor.zk.getData(
                "/watch_test1",
                false,
                new AsyncCallback.DataCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        System.out.println("[GET DATA] rc=" + rc +
                                ", path=" + path +
                                ", value=" + new String(data));

                        // After reading → delete
                        deleteData(executor);
                    }
                },
                null
        );
    }

    // --------------------------------------
    // 4. DELETE
    // --------------------------------------
    static void deleteData(Executor executor) {
        System.out.println("Deleting /watch_test1 ...");

        executor.zk.delete(
                "/watch_test1",
                -1,
                new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        System.out.println("[DELETE] rc=" + rc + ", path=" + path);
                    }
                },
                null
        );
    }
}