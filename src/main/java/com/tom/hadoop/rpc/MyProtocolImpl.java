package com.tom.hadoop.rpc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyProtocolImpl implements MyProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        System.out.println("MyProxy.ProtocolVersion=" + MyProtocol.versionID);
        return MyProtocol.versionID;
    }


    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(MyProtocol.versionID, null);
    }

    @Override
    public String findName(String sno) throws IOException {
        System.out.println( "我被调用了！");
        if(StringUtils.equalsIgnoreCase(sno.trim(), "G20210735010181"))
            return "家乐";
        if(StringUtils.equalsIgnoreCase(sno.trim(), "G20210123456789"))
            return "心心";
        return null;
    }

}
