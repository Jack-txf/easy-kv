package com.feng.easykv.core.rpc;

import com.feng.easykv.core.state.RaftNode;
import com.feng.easykv.protocol.KvRaftProto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: 该节点收到其他节点的消息时的处理
 * @Author: txf
 * @Date: 2026/2/11
 */
public class RpcClientHandler extends SimpleChannelInboundHandler<KvRaftProto.RaftKvMessage> {
    private static final Logger log = LoggerFactory.getLogger(RpcClientHandler.class);

    private final RaftNode raftNode;

    public RpcClientHandler(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvRaftProto.RaftKvMessage msg) {
        // 2.VOTE_RESPONSE 投票请求回来的响应【投票请求是结点作为客户端发出的，应该在客户端的handler处理响应】
        if (msg.getType() == KvRaftProto.RaftKvMessage.MessageType.VOTE_RESPONSE) {
            log.info("receive vote response.........");
            KvRaftProto.VoteResponse voteResponse = msg.getVoteResponse();
            raftNode.tackleVoteResponse(voteResponse);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
