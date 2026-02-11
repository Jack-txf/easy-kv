package com.feng.easykv.core.log;

/**
 * @Description: 日志条目
 * @Author: txf
 * @Date: 2026/2/9
 */
public class LogEntry {
    private final long term;
    private final long index;
    private final String command;

    public LogEntry(long term, long index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    // getter方法
    public long getTerm() { return term; }
    public long getIndex() { return index; }
    public String getCommand() { return command; }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", command='" + command + '\'' +
                '}';
    }
}
