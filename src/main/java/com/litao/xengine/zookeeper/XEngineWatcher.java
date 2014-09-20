package com.litao.xengine.zookeeper;

import com.litao.xengine.config.XEngineConfiguration;
import com.litao.xengine.util.XEngineUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 9/16/14.
 */
public class XEngineWatcher extends ConnectionWatcher {
    private static final String ZOOKEEPER_HOSTS;
    private static final String ROOT_PATH;
    private static final String SERVERS_PATH;

    public static final String SERVER_IP;
    private static final String SERVER_NAME;
    private static final String SERVER_BASE_PATH;

    private static volatile String serverNodePath = null;

    private static XEngineWatcher instance = null;

    private static Logger LOG = LoggerFactory.getLogger(XEngineWatcher.class);

    static {
        SERVER_IP = XEngineUtils.getRealIp();

        ZOOKEEPER_HOSTS = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.quorum");
        ROOT_PATH = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.root.path", "/xengine");
        SERVER_NAME = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.server.name", SERVER_IP);

        if (ZOOKEEPER_HOSTS == null || ZOOKEEPER_HOSTS.equals("")
                || ROOT_PATH == null || ROOT_PATH.equals("")
                || SERVER_NAME == null || SERVER_NAME.equals("")
                || SERVER_IP == null || SERVER_IP.equals("")) {
            LOG.error("Basic configuration is failed to load.");
            System.exit(1);
        }

        SERVERS_PATH = ROOT_PATH + "/servers";
        SERVER_BASE_PATH = SERVERS_PATH + "/" + SERVER_NAME;
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
            prepare();
            serverNodePath = register();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    private void prepare() throws KeeperException, InterruptedException {
        // Create root basic znode
        if (zk.exists(ROOT_PATH, false) == null) {
            zk.create(ROOT_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zk.exists(SERVERS_PATH, false) == null) {
            zk.create(SERVERS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private String register() throws KeeperException, InterruptedException {
        String znodePath = SERVER_BASE_PATH + "_" + zk.getSessionId() + "_";
        return zk.create(znodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public List<String> getActiveServers() throws KeeperException, InterruptedException {
        // FIXME need to call sync() to get latest view
        return zk.getChildren(SERVERS_PATH, false);
    }

    @Override
    protected void handler(WatchedEvent event) {

    }
}
