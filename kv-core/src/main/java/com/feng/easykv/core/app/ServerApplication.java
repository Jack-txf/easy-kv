package com.feng.easykv.core.app;

import com.feng.easykv.core.server.RaftNettyServer;

/**
 * @Description: 节点
 * @Author: txf
 * @Date: 2026/2/9
 */
public class ServerApplication {
    public static void main(String[] args) {
        int port = getPort(args);
        RaftNettyServer raftNettyServer = new RaftNettyServer(port);
        try {
            raftNettyServer.start();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getPort(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("node.port=")) {
                return Integer.parseInt(arg.substring(10));
            }
        }
        return 7777;
    }
}
