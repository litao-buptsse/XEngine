package com.litao.xengine.server;

import com.litao.xengine.zookeeper.XEngineWatcher;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 9/7/14.
 */
public class XEngineMaster {
    private static XEngineWatcher watcher = XEngineWatcher.getInstance();

    private static Logger LOG = LoggerFactory.getLogger(XEngineMaster.class);

    public static void main(String[] args) throws InterruptedException, KeeperException {
        while (true) {
            XEngineWatcher.ServerInfo leader = watcher.getLeader();
            LOG.debug("leader: " + leader);
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
