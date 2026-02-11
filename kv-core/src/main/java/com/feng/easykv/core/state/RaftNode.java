package com.feng.easykv.core.state;

import com.feng.easykv.core.config.NodesConfig;
import com.feng.easykv.core.log.LogManager;
import com.feng.easykv.core.rpc.RpcPeer;
import com.feng.easykv.core.store.MemoryStorage;
import com.feng.easykv.protocol.KvRaftProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description: raft结点
 * @Author: txf
 * @Date: 2026/2/10
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    //
    private final NodesConfig nodesConfig;
    // 集群节点列表（如["127.0.0.1:8081", "127.0.0.1:8082"]）
    private final List<RpcPeer> rpcPeers; // RPC连接

    // 存储部分
    private final MemoryStorage storage;
    private final LogManager logManager;

    // ==================================================选举部分
    public KvRaftProto.VoteResponse tackleVoteRequest(KvRaftProto.VoteRequest voteRequest) {
        // 比较任期
        if (voteRequest.getTerm() < currentTerm.get()) {
            log.info("{} 投票请求任期太小，拒绝投票", nodeId);
            return buildVoteResponse(false, currentTerm.get());
        }
        if ( votedFor != null && !voteRequest.getCandidateId().equals(votedFor) ) {
            log.info("{}已投给其他人，拒绝该投票请求", nodeId);
            return buildVoteResponse(false, currentTerm.get());
        }
        // 再比较日志情况
        if ( voteRequest.getLastLogIndex() >= logManager.getLastLogIndex() &&
         voteRequest.getLastLogTerm() >= logManager.getLastLogTerm() ) {
            log.info("{} 投票请求ok，赞成投票", nodeId);
            currentTerm.set(voteRequest.getTerm()); // 更新自己的任期
            votedFor = voteRequest.getCandidateId(); // 投票给该节点
            return buildVoteResponse(true, voteRequest.getTerm());
        }
        log.info("{} 投票请求日志太旧，拒绝该投票请求", nodeId);
        return buildVoteResponse(false, currentTerm.get());
    }
    private KvRaftProto.VoteResponse buildVoteResponse( boolean agreeOrNot, long term ) {
        return KvRaftProto.VoteResponse.newBuilder()
                .setTerm(term)
                .setVoteGranted(agreeOrNot)
                .build();
    }

    // 投票结果处理,【投票请求是结点作为客户端发出的，要在客户端的handler处理响应】
    public synchronized void tackleVoteResponse(KvRaftProto.VoteResponse voteResponse) {
        long term = voteResponse.getTerm();
        // 2. 发现更高任期，立即降级并更新
        // 1. 任期检查：对方比我大，我立即认输
        if (term > currentTerm.get()) {
            stepDown(term);
            return;
        }
        // 2. 状态检查：如果我已经不是 Candidate 了（比如已经超时重选或收到心跳），忽略
        if (state != NodeState.CANDIDATE) return;

        // 3. 任期匹配检查：确保这是对“当前这一轮”选举的回复
        // 如果收到的响应任期比当前小，说明是之前过期的选举回复，直接丢弃
        if (term < currentTerm.get()) {
            return;
        }

        // 4. 从全局管理器获取当前选举的投票状态
        VoteState voteState = GlobalVoteManager.getVoteState(term);
        if (voteState == null) {
            log.error("未找到任期 {} 的投票记录状态", term);
            return;
        }

        // 5. 如果对方投了赞成票
        if (voteResponse.getVoteGranted()) {
            // 增加票数（这里 AtomicInteger 在 VoteState 内部保证了线程安全，
            // 但由于本方法加了 synchronized，其实双重保险）
            int currentVotes = voteState.addVote();
            int majority = voteState.getMajority();
            log.info("赞成票，当前票数: {}/{}", currentVotes, nodesConfig.getNodeList().size());

            // 6. 检查是否达到多数派
            if (currentVotes >= majority) {
                log.info("节点 {} 获得过半选票 ({})，准备晋升为 Leader", nodeId, currentVotes);
                becomeLeader();
            }
        } else {
            log.info("拒绝了我的投票请求");
        }
    }
    private synchronized void stepDown(long newTerm) {
        log.info("任期过时 (Local: {}, New: {}), 降级为 Follower", currentTerm.get(), newTerm);
        this.state = NodeState.FOLLOWER;
        this.currentTerm.set(newTerm);
        this.votedFor = null;
        resetElectionTimeout();
    }
    private synchronized void becomeLeader() {
        if (state != NodeState.CANDIDATE) return;
        if (state == NodeState.LEADER) return;

        this.state = NodeState.LEADER;
        log.info("Node {} 赢得选举，即将成为 Leader, Term: {}", nodeId, currentTerm.get());
        // 1. 清理上一任期的残留状态
        this.votedFor = null;

        // 2. 立即发送第一波心跳，宣示主权 (防止其他节点又超时)
        sendHeartbeats();

        // 3. 启动定时心跳任务 (比如每 2 秒一次)
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats,
                0, 1000, TimeUnit.MILLISECONDS);
        log.info("<<<<< 节点 {} 正式成为 Term {} 的 Leader >>>>>", nodeId, currentTerm.get());
    }

    private void sendHeartbeats() {
        // 只有 Leader 才能发送心跳
        if (state != NodeState.LEADER) {
            this.heartbeatTask.cancel(true);
            return;
        }
        log.info("Leader {} 正在广播心跳, Term: {}", nodeId, currentTerm.get());
        // 2. 构造通用的心跳请求
        KvRaftProto.AppendEntriesRequest heartbeatReq = KvRaftProto.AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm.get())
                .setLeaderId(nodeId)
                .setPrevLogIndex(logManager.getLastLogIndex())
                .setPrevLogTerm(logManager.getLastLogTerm())
                .setLeaderCommit(0) // TODO
                // entries 为空，这就是心跳的精髓
                .build();
        // 3. 封装成通用消息体
        KvRaftProto.RaftKvMessage raftMsg = KvRaftProto.RaftKvMessage.newBuilder()
                .setType(KvRaftProto.RaftKvMessage.MessageType.APPEND_ENTRIES_REQUEST)
                .setNodeId(nodeId)
                .setAppendEntriesRequest(heartbeatReq)
                .build();
        // 4. 广播发送
        for (RpcPeer peer : rpcPeers) {
            if (!peer.isSelf()) {
                peer.send(raftMsg);
            }
        }
    }

    public synchronized KvRaftProto.AppendEntriesResponse tackleHeartbeat(KvRaftProto.AppendEntriesRequest req) {
        long leaderTerm = req.getTerm();
        // 1. 任期检查：对方任期比我小，直接拒绝（你是过时的 Leader）
        if (leaderTerm < currentTerm.get()) {
            log.info("收到过期心跳 (Term: {}), 我的 Term: {}, 拒绝该请求", leaderTerm, currentTerm.get());
            return buildAppendResponse(currentTerm.get(), false);
        }
        // 2. 发现更新的任期，或者确认合法 Leader
        // 无论我之前是 Candidate 还是 Follower，只要见到更高或相等的任期，都必须服从
        if (leaderTerm > currentTerm.get()) {
            stepDown(leaderTerm); // 更新 Term，状态转为 Follower
        } else {
            // leaderTerm == currentTerm
            // 如果我之前是 Candidate（正在选举中），现在有人选上了，我得乖乖认输退回 Follower
            this.state = NodeState.FOLLOWER;
            this.votedFor = null; // 清除当前任期的投票记录
        }
        // 3. 【核心】重置选举计时器
        // 既然收到了合法 Leader 的心跳，我就不能再发起选举了
        resetElectionTimeout();

        log.debug("收到来自 Leader {} 的心跳，Term: {}, 已重置超时时间", req.getLeaderId(), leaderTerm);
        return buildAppendResponse(currentTerm.get(), true);
    }
    private KvRaftProto.AppendEntriesResponse buildAppendResponse(long l, boolean b) {
        return KvRaftProto.AppendEntriesResponse.newBuilder()
                .setTerm(l)
                .setSuccess(b)
                .build();
    }
    //===================================================================

    // 节点状态
    public enum NodeState { FOLLOWER, CANDIDATE, LEADER }
    private volatile NodeState state = NodeState.FOLLOWER;

    // Raft核心元数据
    private volatile AtomicLong currentTerm = new AtomicLong(0);  // 当前任期
    private volatile String votedFor = null; // 已投票节点
    private volatile long commitIndex = 0;  // 已提交日志索引
    private volatile long lastApplied = 0;  // 已应用到KV的索引

    // Leader专用：每个Follower的日志同步状态
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // 节点配置
    private final String nodeId; // 本节点ID（如"node1"）
    private final int port;      // 本节点端口


    // 配置常量--为了测试，我们把时间调长一点儿
    private static final int MIN_ELECTION_TIMEOUT = 1500; // 选举超时1500-3000ms随机
    private static final int MAX_ELECTION_TIMEOUT = 3000;
    private static final int HEARTBEAT_INTERVAL = 500;    // 心跳间隔50ms

    // 依赖组件
    // private final LogManager logManager; // 高性能日志管理器
    // private final MemoryStorage storage;


    private ScheduledExecutorService scheduler = null;
    private volatile ScheduledFuture<?> heartbeatTask; // Leader心跳定时任务

    // 计时器相关
    private long lastHeartbeatTime;
    private long electionTimeout;

    public RaftNode(int port) {
        this.port = port;

        // 从配置文件中找到自己
        this.nodesConfig = new NodesConfig();
        this.nodeId = nodesConfig.findSelf(port);

        // 需要把自身结点
        this.rpcPeers = nodesConfig.getNodeList().stream()
                 // node的格式是'ip:port'
                 .map(node -> new RpcPeer(node, node.split(":")[0],
                         Integer.parseInt(node.split(":")[1]), this))
                 .toList();

        this.logManager = new LogManager();
        this.storage = new MemoryStorage();

        // 把两个时间先初始化咯
        this.electionTimeout = 8000 + ThreadLocalRandom.current().nextInt(4000);
        this.lastHeartbeatTime = System.currentTimeMillis();
        log.info("初始化选举超时：{}", electionTimeout);

        // 定时器
        scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(this::tick, 2, 2000, TimeUnit.MILLISECONDS);
    }

    // 核心逻辑：时钟滴答
    private void tick() {
        log.info("检查是否超时：{} 状态: {}", nodeId, state);
        try {
            if (state != NodeState.LEADER && isTimeout()) {
                becomeCandidate();
            } else if (state == NodeState.LEADER) {
                // sendHeartbeats();
            }
        } catch ( Exception e ) {
            log.error("{} 节点tick定时任务异常", nodeId, e);
        }
    }
    private void becomeCandidate() {
        log.info("{} 选举超时，转为 Candidate，开始任期: {}", nodeId, currentTerm.get() + 1);
        state = NodeState.CANDIDATE;
        currentTerm.getAndIncrement(); // 任期+1
        votedFor = nodeId; // 给自己投一票
        resetElectionTimeout();
        // 集群发送投票请求
        requestVotes();
    }

    private void requestVotes() {
        // 1. 初始化票数：自己的一票
        AtomicInteger grantedVotes = new AtomicInteger(1);
        long count = rpcPeers.stream().filter(peer -> !peer.isSelf()).count(); // 不包含自己的结点数
        int majority = (int) ((count + 1) / 2 + 1); // 总节点数(包含自己)的半数以上

        // 2.构造投票消息
        KvRaftProto.VoteRequest voteRequest = KvRaftProto.VoteRequest.newBuilder()
                .setTerm(currentTerm.get())
                .setCandidateId(nodeId)
                .setLastLogIndex(logManager.getLastLogIndex())
                .setLastLogTerm(logManager.getLastLogTerm())
                .build();
        // 3. 发送投票请求
        // 构建一个对象，表示当前投票请求的状态
        // String voteId = UUID.randomUUID().toString().replaceAll("-", "");
        // 这里为什么可以用term？因为 Raft 规定，一个节点在一个 term 内只能投一张票。所以，只要 term 匹配，这个响应就一定是针对你当前发起的这一轮选举的。
        GlobalVoteManager.setVoteState(currentTerm.get(), new VoteState(nodeId, currentTerm.get(), majority));

        int countSend = 0;
        for (RpcPeer peer : rpcPeers) {
            if (!peer.isSelf()) { // 不是自身结点，就发送投票请求
                boolean send = peer.send(KvRaftProto.RaftKvMessage.newBuilder()
                        .setType(KvRaftProto.RaftKvMessage.MessageType.VOTE_REQUEST)
                        .setVoteRequest(voteRequest)
                        .build());
                if ( send ) countSend++;
            }
        }
        log.info("发送投票请求：{}，已发送给了 {} 个结点..", voteRequest, countSend);
    }

    private void resetElectionTimeout() {
        // 8000ms ~ 12000ms 随机超时，避免平票
        this.electionTimeout = 8000 + ThreadLocalRandom.current().nextInt(4000);
        log.info("重置 {} 节点选举时间，随机超时：{} ms", nodeId, electionTimeout);
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    private boolean isTimeout() {
        return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
    }


    public String getNodeId() { return nodeId; }
    public int getPort() { return port; }

}
