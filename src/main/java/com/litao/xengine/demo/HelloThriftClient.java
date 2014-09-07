package com.litao.xengine.demo;

import com.litao.xengine.demo.api.HelloThriftService;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by Tao Li on 9/7/14.
 */
public class HelloThriftClient {
    public static final String SERVER_IP = "localhost";
    public static final int SERVER_PORT = 9001;
    public static final int TIMEOUT = 30000;

    private static Logger LOG = Logger.getLogger(HelloThriftClient.class);

    public void startClient(String userName) {
        TTransport transport = null;
        try {
            transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);
            TProtocol protocol = new TBinaryProtocol(transport);
            HelloThriftService.Client client = new HelloThriftService.Client(protocol);
            transport.open();

            String result = client.sayHello(userName);
            LOG.info("Thrift client result: " + result);
        } catch(TTransportException e) {
            LOG.error(e.getMessage(), e);
        } catch (TException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if(transport != null) {
                transport.close();
            }
        }
    }

    public static void main(String[] args) {
        HelloThriftClient client = new HelloThriftClient();
        client.startClient("litao");
    }
}
