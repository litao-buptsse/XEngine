package com.litao.xengine.demo;

import com.litao.xengine.demo.api.HelloThriftService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tao Li on 9/7/14.
 */
public class HelloThriftServer {
    public static final int SERVER_PORT = 9001;

    private static Logger LOG = LoggerFactory.getLogger(HelloThriftServer.class);

    public void startServer() {
        try {
            LOG.info("HelloThriftServer start...");

            TProcessor tprocessor = new HelloThriftService.Processor<HelloThriftService.Iface>(new HelloThriftmpl());
            TServerSocket serverTransport = new TServerSocket(SERVER_PORT);
            TServer.Args tArgs = new TServer.Args(serverTransport);
            tArgs.processor(tprocessor);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());

            TServer server = new TSimpleServer(tArgs);
            server.serve();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        HelloThriftServer server = new HelloThriftServer();
        server.startServer();
    }
}
