package com.litao.xengine.zookeeper;

import com.litao.xengine.config.XEngineConfiguration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Tao Li on 9/15/14.
 */
public abstract class BasicWatcher implements Watcher {
    private static final int SESSION_TIMEOUT = XEngineConfiguration.CONFIG.getInt("xengine.zookeeper.session.timeout", 5000);

    protected ZooKeeper zk = null;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    protected void connect(String hosts) throws InterruptedException, IOException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    protected abstract void handler(WatchedEvent event);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
        handler(event);
    }
}
