package com.litao.xengine.zookeeper;

import com.litao.xengine.config.XEngineConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 9/16/14.
 */
public class XEngineWatcher extends ConnectionWatcher {
    private static String ZOOKEEPER_HOSTS = null;
    private static String ROOT_PATH = null;
    private static String SERVER_NAME = null;
    private static String SERVERS_PATH = null;
    private static String SERVER_PATH = null;

    private static XEngineWatcher instance = null;

    private static Logger LOG = LoggerFactory.getLogger(XEngineWatcher.class);

    static {
        ZOOKEEPER_HOSTS = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.quorum");
        ROOT_PATH = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.root.path", "/xengine");
        SERVER_NAME = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.server.name");

        if (ZOOKEEPER_HOSTS == null || ZOOKEEPER_HOSTS.equals("")
                || ROOT_PATH == null || ROOT_PATH.equals("")
                || SERVER_NAME == null || SERVER_NAME.equals("")) {
            LOG.error("Basic configuration is failed to load.");
            System.exit(1);
        }

        SERVERS_PATH = ROOT_PATH + "/servers";
        SERVER_PATH = SERVERS_PATH + "/" + SERVER_NAME;
    }

    public static XEngineWatcher getInstance() {
        // double checked locking technique to implement a thread-safe singleton
        if (instance == null) {
            synchronized (XEngineWatcher.class) {
                if (instance == null) {
                    instance = new XEngineWatcher();
                }
            }
        }
        return instance;
    }

    private XEngineWatcher() {
        try {
            connect(ZOOKEEPER_HOSTS);
            if (zk.exists(ROOT_PATH, false) == null) {
                zk.create(ROOT_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(SERVERS_PATH, false) == null) {
                zk.create(SERVERS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            registerServer();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    private String registerServer() throws KeeperException, InterruptedException {
        return zk.create(SERVER_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public static void main(String[] args) throws InterruptedException {
        XEngineWatcher watcher = XEngineWatcher.getInstance();
        TimeUnit.SECONDS.sleep(20);
        watcher.close();
    }
}
