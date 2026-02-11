package com.feng.easykv.core.server.handler;

import com.feng.easykv.core.state.RaftNode;
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
public class KvBusinessHandler extends SimpleChannelInboundHandler<KvRaftProto.RaftKvMessage> {
    private static final Logger log = LoggerFactory.getLogger(KvBusinessHandler.class);
    private RaftNode node;

    public KvBusinessHandler(RaftNode node) {
        this.node = node;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KvRaftProto.RaftKvMessage raftKvMessage) throws Exception {
        // log.info("server received message：type is {}, and content is {}", raftKvMessage.getType(), raftKvMessage);
        // 然后根据不同的消息类型做不同的业务逻辑处理
        /*
          总共有：
           1.KV_REQUEST 接受客户端的请求set, get, remove
           2.VOTE_REQUEST 接受投票请求 node结点发送来的
           3.APPEND_ENTRIES_REQUEST 接受leader发送来的日志追加【只有Follower才能收到】

            TODO 下面是三个响应：【不是在这里处理的，因为下面三个请求】
            1.KV_RESPONSE 响应客户端的请求
            2.VOTE_RESPONSE 投票请求回来的响应，不该在这里处理
            3.APPEND_ENTRIES_RESPONSE 响应日志追加【只有Leader才能收到】
         */
        // 1.如果是投票请求
        if ( raftKvMessage.getType() == KvRaftProto.RaftKvMessage.MessageType.VOTE_REQUEST) {
            log.info("receive vote request.........");
            KvRaftProto.VoteRequest voteRequest = raftKvMessage.getVoteRequest();
            KvRaftProto.VoteResponse voteResponse = node.tackleVoteRequest(voteRequest);
            ctx.writeAndFlush(KvRaftProto.RaftKvMessage.newBuilder()
                    .setType(KvRaftProto.RaftKvMessage.MessageType.VOTE_RESPONSE)
                    .setVoteResponse(voteResponse)
                    .build());
        }

        // 2.如果是Leader发过来的心跳、或者是日志追加请求
        if ( raftKvMessage.getType() == KvRaftProto.RaftKvMessage.MessageType.APPEND_ENTRIES_REQUEST ) {

            // 2.1 如果是Leader发过来的心跳
            if (raftKvMessage.getAppendEntriesRequest().getEntriesList().isEmpty()) {
                log.info("receive heartbeat.........");
                KvRaftProto.AppendEntriesResponse appendEntriesResponse =
                        node.tackleHeartbeat(raftKvMessage.getAppendEntriesRequest()); // 处理心跳
                ctx.writeAndFlush(KvRaftProto.RaftKvMessage.newBuilder()
                        .setType(KvRaftProto.RaftKvMessage.MessageType.APPEND_ENTRIES_RESPONSE)
                        .setAppendEntriesResponse(appendEntriesResponse)
                        .build());
            }
        }
    }
}
