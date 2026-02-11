package com.feng.easykv.core.state;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description: 投票状态
 * @Author: txf
 * @Date: 2026/2/11
 */
public class VoteState {
    private String candidate; // 候选者是谁--nodeId
    private long term; // 投票的任期
    private AtomicInteger voteCount; // 已获得的投票

    public int getMajority() {
        return majority;
    }

    private int majority;

    public VoteState(String candidate, long term, int majority) {
        this.candidate = candidate;
        this.term = term;
        this.voteCount = new AtomicInteger(1);
        this.majority = majority;
    }
    public int addVote() {
        return this.voteCount.getAndIncrement() + 1;
    }
    public String getCandidate() {
        return candidate;
    }
    public long getTerm() {
        return term;
    }
    public int getVoteCount() {
        return voteCount.get();
    }

}
