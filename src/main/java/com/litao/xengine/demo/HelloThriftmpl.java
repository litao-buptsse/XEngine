package com.litao.xengine.demo;

import com.litao.xengine.demo.api.HelloThriftService;
import org.apache.thrift.TException;

/**
 * Created by Tao Li on 9/7/14.
 */
public class HelloThriftmpl implements HelloThriftService.Iface {

    @Override
    public String sayHello(String username) throws TException {
        return "Hi " + username + ", Welcome to guaver.info";
    }
}
