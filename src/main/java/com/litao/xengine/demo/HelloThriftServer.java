package com.litao.xengine.demo;

import com.litao.xengine.demo.api.HelloThriftService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
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


            // 1. 简单的单线程服务模型
            // TServerSocket serverTransport = new TServerSocket(SERVER_PORT);
            // TServer.Args tArgs = new TServer.Args(serverTransport);
            // tArgs.processor(tprocessor);
            // tArgs.protocolFactory(new TBinaryProtocol.Factory());
            // TServer server = new TSimpleServer(tArgs);

            // 2. 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求
            // TServerSocket serverTransport = new TServerSocket(SERVER_PORT);
            // TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(serverTransport);
            // ttpsArgs.processor(tprocessor);
            // ttpsArgs.protocolFactory(new TBinaryProtocol.Factory());
            // TServer server = new TThreadPoolServer(ttpsArgs);

            // 3. 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
            TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(SERVER_PORT);
            TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(tnbSocketTransport);
            tnbArgs.processor(tprocessor);
            tnbArgs.transportFactory(new TFramedTransport.Factory());
            tnbArgs.protocolFactory(new TCompactProtocol.Factory());
            TServer server = new TNonblockingServer(tnbArgs);

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
