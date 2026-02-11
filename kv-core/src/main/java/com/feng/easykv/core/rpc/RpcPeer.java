package com.feng.easykv.core.rpc;

import com.feng.easykv.core.state.RaftNode;
import com.feng.easykv.core.utils.NetworkUtil;
import com.feng.easykv.protocol.KvRaftProto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Description: 与其他节点连接
 * @Author: txf
 * @Date: 2026/2/11
 */
public class RpcPeer {
    private static final Logger log = LoggerFactory.getLogger(RpcPeer.class);

    private boolean isSelf = false;
    private final String peerId;      // 目标节点ID
    private final String host;        // 目标IP
    private final int port;           // 目标端口

    private EventLoopGroup group;
    private volatile Channel channel; // 持有的长连接
    private final RaftNode raftNode;  // 回传消息给本地节点

    public RpcPeer(String peerId, String host, int port, RaftNode raftNode) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.raftNode = raftNode;
        if ( !judgeIsSelf(peerId)) { // 如果不是自己，则建立长连接
            this.group = new NioEventLoopGroup(1);
            connect();
        } else { // 如果是自身，则不建立长连接
            this.isSelf = true;
        }
    }

    // 判断是不是自己
    public boolean isSelf() {
        return isSelf;
    }
    public boolean judgeIsSelf( String peerId ) {
        String[] sp = peerId.split(":");
        String ip = sp[0];
        int port = Integer.parseInt(sp[1]);
        if ( "127.0.0.1".equals(ip) ) {
            return port == raftNode.getPort();
        } else {
            List<String> localIpv4List = NetworkUtil.getValidLocalIpv4List();
            for (String ipv4 : localIpv4List) {
                if ( ipv4.equals(ip) ) {
                    return port == raftNode.getPort();
                }
            }
        }
        return false;
    }

    /**
     * 连接/重连逻辑
     */
    public synchronized void connect() {
        if (channel != null && channel.isActive()) return;

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // 1. Protobuf 编解码器（和你定义的 proto 对齐）
                        p.addLast(new ProtobufVarint32FrameDecoder());
                        p.addLast(new ProtobufDecoder(KvRaftProto.RaftKvMessage.getDefaultInstance()));
                        p.addLast(new ProtobufVarint32LengthFieldPrepender());
                        p.addLast(new ProtobufEncoder());
                        // 2. 客户端处理器：处理从对方返回的消息
                        p.addLast(new RpcClientHandler(raftNode));
                    }
                });

        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                this.channel = future.channel();
                log.info("connect to server {} success", host + ":" + port);
            } else {
                log.warn("connect to server {} failed, now reconnecting....", host + ":" + port);
                // 连接失败，3秒后重连
                future.channel().eventLoop()
                        .schedule(this::connect, 8, TimeUnit.SECONDS);
            }
        });
    }

    /**
     * 发送消息并返回 Future
     */
    public boolean send(KvRaftProto.RaftKvMessage msg) {
        if (channel != null && channel.isActive()) {
            log.info("send message to server {}", host + ":" + port);
            channel.writeAndFlush(msg);
            return true;
        } else {
            // 简单处理：如果连接没好，直接丢弃，等待下一波心跳/选举超时重试
            connect();
        }
        return false;
    }
}
