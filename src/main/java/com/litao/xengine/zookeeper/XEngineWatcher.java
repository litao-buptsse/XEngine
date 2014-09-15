package com.litao.xengine.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 9/16/14.
 */
public class XEngineWatcher extends ConnectionWatcher {
    private static String ZOOKEEPER_HOSTS = "localhost";
    private static String ROOT_PATH = "/xengine";
    private static String SERVERS_PATH = ROOT_PATH + "/servers";

    private static String serverName = "zoo";
    private static String serverPath = SERVERS_PATH + "/" + serverName;

    private static XEngineWatcher instance = null;

    private XEngineWatcher() {
        try {
            connect(ZOOKEEPER_HOSTS);
            if(zk.exists(ROOT_PATH, false) == null) {
                zk.create(ROOT_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if(zk.exists(SERVERS_PATH, false) == null) {
                zk.create(SERVERS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            registerServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (KeeperException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static XEngineWatcher getInstance() {
        if (instance == null) {
            synchronized (XEngineWatcher.class) {
                if (instance == null) {
                    instance = new XEngineWatcher();
                }
            }
        }
        return instance;
    }

    private void registerServer() throws KeeperException, InterruptedException {
        zk.create(serverPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public static void main(String[] args) throws InterruptedException {
        XEngineWatcher watcher = XEngineWatcher.getInstance();
        TimeUnit.SECONDS.sleep(20);
        watcher.close();
    }
}
