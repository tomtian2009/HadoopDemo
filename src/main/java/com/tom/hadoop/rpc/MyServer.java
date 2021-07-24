package com.tom.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import java.io.IOException;

public class MyServer {

    public static int PORT = 9000;
    public static String IPAddress = "127.0.0.1";

    public static void main(String[] args){

        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress(IPAddress);
        builder.setPort(PORT);
        builder.setNumHandlers(5);
        builder.setProtocol(MyProtocol.class);
        builder.setInstance(new MyProtocolImpl());

        try {
            RPC.Server server = builder.build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

/*        Configuration conf = new Configuration() ;
        RPC.Server server = null;
        try {
            server = new RPC.Builder(conf).setProtocol(MyProtocol.class).setInstance(new MyProtocolImpl()).
                    setBindAddress("localhost").setPort(9000).setNumHandlers(5).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        server.start();*/

    }
}