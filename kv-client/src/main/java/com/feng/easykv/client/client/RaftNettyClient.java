package com.feng.easykv.client.client;

import com.feng.easykv.client.client.handler.ClientBusinessHandler;
import com.feng.easykv.protocol.KvRaftProto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: 客户端
 * @Author: txf
 * @Date: 2026/2/9
 */
public class RaftNettyClient {

    private static final Logger log = LoggerFactory.getLogger(RaftNettyClient.class);

    private Channel channel;

    public RaftNettyClient() {
        start();
    }

    private void start() {
        EventLoopGroup w = new NioEventLoopGroup();
        Bootstrap c;
        try {
            c = new Bootstrap();
            c.group(w)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(KvRaftProto.RaftKvMessage.getDefaultInstance()))
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder())
                                .addLast(new ClientBusinessHandler());
                    }
                });

            ChannelFuture future = c.connect("127.0.0.1", 9999)
                    .addListener( (ChannelFutureListener)f -> {
                        if (f.isSuccess()) {
                            log.info("client node start success, connected server at port：{}", 9999);
                            channel = f.channel();

                            // 注册关闭监听器
                            f.channel().closeFuture().addListener((ChannelFutureListener) closeFuture -> {
                                log.info("Connection closed");
                                w.shutdownGracefully();
                            });
                        } else {
                            log.error("Failed to connect to server", f.cause());
                            w.shutdownGracefully();
                        }
            });
            log.info("client node start success, connected server at port：{}", 9999);
            channel = future.channel();
            // future.channel().closeFuture().sync();
        } catch ( Exception e ) {
            log.error("启动客户端异常: ", e);
        }
    }

    public KvRaftProto.RaftKvMessage send( KvRaftProto.RaftKvMessage message ) {
        if ( channel == null ) {
            log.error("channel is null");
        } else {
            channel.writeAndFlush(message); // 客户端发送消息
        }
        return null;
    }
}
