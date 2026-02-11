package com.feng.easykv.core.server;

// import com.feng.easykv.core.server.handler.RaftServerHandler;
import com.feng.easykv.core.server.handler.KvBusinessHandler;
import com.feng.easykv.core.state.RaftNode;
import com.feng.easykv.protocol.KvRaftProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: 基于Netty实现的网络服务
 * @Author: txf
 * @Date: 2026/1/28
 */
public class RaftNettyServer {
    private static final Logger log = LoggerFactory.getLogger(RaftNettyServer.class);

    private final int port;
    private final RaftNode node;

    public RaftNettyServer(int port) {
        this.port = port;
        this.node = new RaftNode(port);
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder()) // Protobuf粘包拆包解码器
                                    // Protobuf解码器：指定目标消息类型
                                    .addLast(new ProtobufDecoder(KvRaftProto.RaftKvMessage.getDefaultInstance()))
                                    .addLast(new ProtobufVarint32LengthFieldPrepender()) // 编码器-给消息加一个长度
                                    .addLast(new ProtobufEncoder()) // Protobuf编码器
                                    .addLast(new KvBusinessHandler(node));
                        }
                    });
            ChannelFuture future = b.bind(port).sync();
            log.info("server node start at port：{}", port);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
