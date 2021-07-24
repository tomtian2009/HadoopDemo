package com.tom.hadoop.rpc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient {


    public static void main(String[] args)  {

        try {
            InetSocketAddress addr = new InetSocketAddress(MyServer.IPAddress, MyServer.PORT);
            MyProtocol proxy = RPC.getProxy(MyProtocol.class, MyProtocol.versionID, addr, new Configuration());
            String name = proxy.findName(args[0]);
            if (StringUtils.isNotBlank(name)) System.out.println("您的姓名是："+name);
            else System.out.println("未查到您的姓名");
        } catch (IOException e) {
            e.printStackTrace();
        }



    }
}
