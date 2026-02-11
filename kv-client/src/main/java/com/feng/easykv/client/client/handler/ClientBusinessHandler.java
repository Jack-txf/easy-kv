package com.feng.easykv.client.client.handler;

import com.feng.easykv.protocol.KvRaftProto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description:
 * @Author: txf
 * @Date: 2026/2/9
 */
public class ClientBusinessHandler extends SimpleChannelInboundHandler<KvRaftProto.RaftKvMessage> {
    private static final Logger log = LoggerFactory.getLogger(ClientBusinessHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvRaftProto.RaftKvMessage raftKvMessage) throws Exception {
        log.info("client received message：{}", raftKvMessage);
    }
}
