package com.litao.xengine.zookeeper;

import com.litao.xengine.config.XEngineConfiguration;
import com.litao.xengine.util.XEngineUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 9/16/14.
 */
public class XEngineWatcher extends BasicWatcher {
    private static final String ZOOKEEPER_QUORUM;
    private static final String ROOT_PATH;
    private static final String SERVERS_PATH;
    private static final String SERVER_IP;
    private static final String SERVER_NAME;

    private volatile ServerInfo currentServerInfo = new ServerInfo();

    private static XEngineWatcher instance = null;

    private static Logger LOG = LoggerFactory.getLogger(XEngineWatcher.class);

    static {
        SERVER_IP = XEngineUtils.getRealIp();
        ZOOKEEPER_QUORUM = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.quorum");
        ROOT_PATH = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.root.path", "/xengine");
        SERVERS_PATH = ROOT_PATH + "/servers";
        SERVER_NAME = XEngineConfiguration.CONFIG.getString("xengine.zookeeper.server.name", SERVER_IP);

        if (ZOOKEEPER_QUORUM == null || ZOOKEEPER_QUORUM.equals("")
                || ROOT_PATH == null || ROOT_PATH.equals("")
                || SERVER_NAME == null || SERVER_NAME.equals("")) {
            LOG.error("Basic configuration is failed to load.");
            System.exit(1);
        }
    }

    public class ServerInfo {
        String serverName;
        long sessionId;
        String sequenceId;

        public ServerInfo() {

        }

        public ServerInfo(String serverName, long sessionId, String sequenceId) {
            this.serverName = serverName;
            this.sessionId = sessionId;
            this.sequenceId = sequenceId;
        }

        public String getZNodePath() {
            return String.format("%s_%s_%s", serverName, sessionId, sequenceId);
        }

        public String getFullZnodePath() {
            return String.format("%s/%s", SERVERS_PATH, getZNodePath());
        }

        @Override
        public String toString() {
            return "ServerInfo{" +
                    "serverName='" + serverName + '\'' +
                    ", sessionId='" + sessionId + '\'' +
                    ", sequenceId='" + sequenceId + '\'' +
                    '}';
        }
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
        currentServerInfo.serverName = SERVER_NAME;
        try {
            connect(ZOOKEEPER_QUORUM);
            prepareBasicEnv();
            if (isRegistered()) {
                LOG.error("Server was already started, " + currentServerInfo.serverName);
                System.exit(1);
            } else {
                register();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    private void prepareBasicEnv() throws KeeperException, InterruptedException {
        // Create root basic znode
        if (zk.exists(ROOT_PATH, false) == null) {
            zk.create(ROOT_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zk.exists(SERVERS_PATH, false) == null) {
            zk.create(SERVERS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private List<ServerInfo> getAllServerInfos() throws KeeperException, InterruptedException {
        // FIXME need to call sync() to get latest view
        List<ServerInfo> serverInfos = new ArrayList<ServerInfo>();
        for (String server : zk.getChildren(SERVERS_PATH, false)) {
            String[] info = server.split("_");
            if (info.length == 3) {
                serverInfos.add(new ServerInfo(info[0], Long.parseLong(info[1]), info[2]));
            } else {
                String path = SERVERS_PATH + "/" + server;
                LOG.error("Invalid server znode path: " + path);
            }
        }
        return serverInfos;
    }

    private List<ServerInfo> getServerInfosByServerName(String serverName) throws KeeperException, InterruptedException {
        List<ServerInfo> filteredServerInfos = new ArrayList<ServerInfo>();
        for (ServerInfo serverInfo : getAllServerInfos()) {
            if (serverName.equals(serverInfo.serverName)) {
                filteredServerInfos.add(serverInfo);
            }
        }
        return filteredServerInfos;
    }

    private boolean isRegistered() throws KeeperException, InterruptedException {
        return getServerInfosByServerName(currentServerInfo.serverName).size() > 0 ? true : false;
    }

    private void unRegister() throws KeeperException, InterruptedException {
        for (ServerInfo serverInfo : getServerInfosByServerName(currentServerInfo.serverName)) {
            zk.delete(serverInfo.getFullZnodePath(), -1);
        }
    }

    private void register() throws KeeperException, InterruptedException {
        currentServerInfo.sessionId = zk.getSessionId();
        String zNodeBasePath = String.format("%s/%s_%s_", SERVERS_PATH, currentServerInfo.serverName, zk.getSessionId());
        String path = zk.create(zNodeBasePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String[] info = path.split("_");
        if (info.length == 3) {
            currentServerInfo.sequenceId = info[2];
        }
        zk.exists(currentServerInfo.getFullZnodePath(), true);
    }

    public ServerInfo getLeader() throws KeeperException, InterruptedException {
        List<ServerInfo> serverInfos = getAllServerInfos();
        ServerInfo leader = null;
        for (ServerInfo serverInfo : serverInfos) {
            if (leader == null) {
                leader = serverInfo;
            } else {
                if (serverInfo.sequenceId.compareTo(leader.sequenceId) < 0) {
                    leader = serverInfo;
                }
            }
        }
        return leader;
    }

    @Override
    protected void handler(WatchedEvent event) {
        if(event.getType() == Event.EventType.NodeDeleted) {
            if(event.getPath().equals(currentServerInfo.getFullZnodePath())) {
                LOG.error("Current server was killed, because the znode was removed, "
                        + currentServerInfo.getFullZnodePath());
                System.exit(1);
            }
        }
    }
}
