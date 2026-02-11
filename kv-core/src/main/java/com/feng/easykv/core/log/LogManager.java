package com.feng.easykv.core.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 日志存储与管理
 * @Author: txf
 * @Date: 2026/2/9
 */
public class LogManager {
    private List<LogEntry> logEntries;

    // 日志文件路径
    private static final String LOG_FILE_PATH = "easy_kv_log.dat";
    // 内存映射的分段大小（128MB，可根据内存调整）,这里先调整为两兆
    private static final int MAPPED_SIZE = 2 * 1024 * 1024;
    // 文件打开模式：rw = 读写
    private static final String FILE_MODE = "rw";

    private final File logFile;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    // 当前映射的内存缓冲区
    private MappedByteBuffer currentMappedBuffer;
    // 当前映射段的起始偏移（文件偏移）
    private long currentMappedOffset = 0;
    // 当前写入的位置（相对于文件的总偏移）
    private long writePosition = 0;

    public LogManager() {
        this.logFile = new File(LOG_FILE_PATH);
        initFileChannel();
        initMappedBuffer();
        // 初始化时定位到文件末尾（继续追加写）
        try {
            this.writePosition = fileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException("获取文件大小失败", e);
        }
        logEntries = new ArrayList<>();
    }

    /**
     * 追加写入单条日志（核心高性能写入）
     * @param term Raft任期
     * @param index 日志索引
     * @param command KV操作命令（如"PUT key value"）
     */
    public void appendLogEntry(long term, long index, String command) {
        // 1. 准备命令字节数组
        byte[] commandBytes = command.getBytes(StandardCharsets.UTF_8);
        int commandLength = commandBytes.length;
        // 2. 计算总长度
        int totalLength = 4 + 8 + 8 + 4 + commandLength;

        // 3. 准备直接缓冲区（堆外内存，避免拷贝）
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(totalLength);
        // 按格式写入缓冲区 -- 这里是写入二进制的，文件内容我们人类就读不懂了
        directBuffer.putInt(totalLength);
        directBuffer.putLong(term);
        directBuffer.putLong(index);
        directBuffer.putInt(commandLength);
        directBuffer.put(commandBytes);

        // 这里是写入字符串的
        // directBuffer.put((term + index + command).getBytes(StandardCharsets.UTF_8));

        // 翻转缓冲区（从写模式转为读模式）
        directBuffer.flip();
        // 4. 写入到内存映射缓冲区（核心：零拷贝）
        writeToMappedBuffer(directBuffer);        // 5. 更新全局写入位置
        writePosition += totalLength;
    }

    /**
     * 将缓冲区数据写入内存映射区（自动扩容映射段）
     */
    private void writeToMappedBuffer(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            // 检查当前映射缓冲区是否有足够剩余空间
            if (currentMappedBuffer.remaining() < buffer.remaining()) {
                // 先写入当前映射区的剩余空间
                int remaining = currentMappedBuffer.remaining();
                byte[] temp = new byte[remaining];
                buffer.get(temp);
                currentMappedBuffer.put(temp);
                // 强制刷盘（将映射内存的数据同步到磁盘，可选：批量刷盘可提升性能）
                currentMappedBuffer.force();
                // 扩容映射段
                initMappedBuffer();
            } else {
                // 直接写入映射缓冲区
                currentMappedBuffer.put(buffer);
            }
        }
    }

    /**
     * 读取指定索引的日志条目（高性能读取）
     */
    public LogEntry readLogEntry(long index) {
        try {
            // 使用FileChannel + 直接缓冲区读取
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB直接缓冲区
            long fileOffset = 0;
            long fileSize = fileChannel.size();

            while (fileOffset < fileSize) {
                // 重置缓冲区
                directBuffer.clear();
                // 从文件指定偏移读取数据到缓冲区
                int readBytes = fileChannel.read(directBuffer, fileOffset);
                if (readBytes == -1) break;
                directBuffer.flip();

                // 解析缓冲区中的日志条目
                while (directBuffer.hasRemaining()) {
                    // 检查剩余字节是否足够读取固定头部（4+8+8+4=24字节）
                    if (directBuffer.remaining() < 24) break;

                    // 读取固定字段
                    int totalLength = directBuffer.getInt();
                    long term = directBuffer.getLong();
                    long currentIndex = directBuffer.getLong();
                    int commandLength = directBuffer.getInt();

                    // 检查剩余字节是否足够读取command
                    if (directBuffer.remaining() < commandLength) {
                        // 回退缓冲区position，下次继续解析
                        directBuffer.position(directBuffer.position() - 24);
                        break;
                    }

                    // 读取command
                    byte[] commandBytes = new byte[commandLength];
                    directBuffer.get(commandBytes);
                    String command = new String(commandBytes, StandardCharsets.UTF_8);

                    // 校验总长度
                    int actualLength = 4 + 8 + 8 + 4 + commandLength;
                    if (totalLength != actualLength) {
                        throw new RuntimeException("日志文件损坏：总长度不匹配");
                    }

                    // 找到目标索引则返回
                    if (currentIndex == index) {
                        return new LogEntry(term, index, command);
                    }

                    // 更新文件偏移
                    fileOffset += totalLength;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("读取日志条目失败 [index=" + index + "]", e);
        }
        return null;
    }

    /**
     * 加载所有日志条目（节点启动时恢复）
     */
    public List<LogEntry> loadAllLogEntries() {
        List<LogEntry> logEntries = new ArrayList<>();
        try {
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024 * 1024);
            long fileOffset = 0;
            long fileSize = fileChannel.size();

            while (fileOffset < fileSize) {
                directBuffer.clear();
                int readBytes = fileChannel.read(directBuffer, fileOffset);
                if (readBytes == -1) break;
                directBuffer.flip();

                while (directBuffer.hasRemaining()) {
                    if (directBuffer.remaining() < 24) break;

                    int totalLength = directBuffer.getInt();
                    long term = directBuffer.getLong();
                    long index = directBuffer.getLong();
                    int commandLength = directBuffer.getInt();

                    if (directBuffer.remaining() < commandLength) {
                        directBuffer.position(directBuffer.position() - 24);
                        break;
                    }

                    byte[] commandBytes = new byte[commandLength];
                    directBuffer.get(commandBytes);
                    String command = new String(commandBytes, StandardCharsets.UTF_8);

                    int actualLength = 4 + 8 + 8 + 4 + commandLength;
                    if (totalLength != actualLength) {
                        throw new RuntimeException("日志文件损坏：总长度不匹配");
                    }

                    logEntries.add(new LogEntry(term, index, command));
                    fileOffset += totalLength;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("加载所有日志条目失败", e);
        }
        return logEntries;
    }

    /**
     * 强制刷盘（将映射内存的数据同步到磁盘）
     */
    public void forceFlush() {
        if (currentMappedBuffer != null) {
            currentMappedBuffer.force(); // 同步映射内存到磁盘
        }
        try {
            fileChannel.force(true); // 强制刷盘（包含元数据）
        } catch (IOException e) {
            throw new RuntimeException("刷盘失败", e);
        }
    }

    /**
     * 关闭资源（必须调用，否则会导致文件句柄泄漏）
     */
    public void close() {
        try {
            forceFlush();
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("关闭资源失败", e);
        }
    }


    /**
     * 初始化/扩容内存映射缓冲区
     */
    private void initMappedBuffer() {
        try {
            // 计算需要映射的起始位置和大小
            long fileSize = fileChannel.size();
            // 如果当前映射段已写满，或首次初始化，创建新的映射
            if (currentMappedBuffer == null || writePosition >= currentMappedOffset + MAPPED_SIZE) {
                currentMappedOffset = (writePosition / MAPPED_SIZE) * MAPPED_SIZE;
                // 映射文件的指定区间到内存（FileChannel.MapMode.READ_WRITE：读写模式）
                currentMappedBuffer = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        currentMappedOffset,
                        MAPPED_SIZE
                );
                // 将缓冲区的position定位到当前写入位置相对于映射段的偏移
                currentMappedBuffer.position((int) (writePosition - currentMappedOffset));
            }
        } catch (IOException e) {
            throw new RuntimeException("初始化内存映射缓冲区失败", e);
        }
    }

    /**
     * 初始化FileChannel（核心高性能通道）
     */
    private void initFileChannel() {
        try {
            // 不存在则创建文件
            if (!logFile.exists()) {
                boolean newFile = logFile.createNewFile();
                if (!newFile) {
                    throw new RuntimeException("创建文件失败");
                }
            }
            this.raf = new RandomAccessFile(logFile, FILE_MODE);
            this.fileChannel = raf.getChannel();
        } catch (IOException e) {
            throw new RuntimeException("初始化FileChannel失败", e);
        }
    }

    public long getLastLogIndex() {
        return !logEntries.isEmpty() ? logEntries.getLast().getIndex() : 0;
    }

    public long getLastLogTerm() {
        return !logEntries.isEmpty() ? logEntries.getLast().getTerm() : 0;
    }

}
