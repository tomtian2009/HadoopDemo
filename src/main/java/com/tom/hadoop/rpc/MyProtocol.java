package com.tom.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;
import java.io.IOException;

/**
 * 自定义的 protocol 协议
 */
public interface MyProtocol extends VersionedProtocol {

    public static final long versionID = 1235L ;
    public String findName(String sno) throws IOException;

}
