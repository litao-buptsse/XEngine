package com.litao.xengine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Created by Tao Li on 2014/9/20.
 */
public class XEngineUtils {
    private static Logger LOG = LoggerFactory.getLogger(XEngineUtils.class);

    public static String getRealIp() {
        String localip = null;// ����IP�����û����������IP�򷵻���
        String netip = null;// ����IP
        try {
            Enumeration<NetworkInterface> netInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            boolean finded = false;// �Ƿ��ҵ�����IP
            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = address.nextElement();
                    if (!ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && ip.getHostAddress().indexOf(":") == -1) {// ����IP
                        netip = ip.getHostAddress();
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && ip.getHostAddress().indexOf(":") == -1) {// ����IP
                        localip = ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Get IP Failed");
        }
        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }
}
