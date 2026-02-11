package com.feng.easykv.core.state;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 全局投票管理
 * @Author: txf
 * @Date: 2026/2/11
 */
public class GlobalVoteManager {
    private static final ConcurrentHashMap<Long, VoteState> voteStateMap = new ConcurrentHashMap<>(8);

    public static VoteState getVoteState(Long nodeId) {
        return voteStateMap.get(nodeId);
    }

    public static void setVoteState(Long nodeId, VoteState voteState) {
        voteStateMap.put(nodeId, voteState);
    }

    public static void removeVoteState(Long nodeId) {
        voteStateMap.remove(nodeId);
    }
}
