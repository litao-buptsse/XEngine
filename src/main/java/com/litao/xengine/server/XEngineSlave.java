package com.litao.xengine.server;

import com.litao.xengine.zookeeper.XEngineWatcher;

import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 9/7/14.
 */
public class XEngineSlave {
    private static XEngineWatcher watcher = XEngineWatcher.getInstance();

    public static void main(String[] args) throws InterruptedException {
        TimeUnit.SECONDS.sleep(20);
    }
}
