# 简易的分布式kv设计--(一)

**这篇文章目前只设计到集群启动，然后自动选主的功能。**

地址：https://github.com/Jack-txf/easy-kv

**TIPS**：此文章对应的分支版本是**version0211**

raft基础知识：https://mp.weixin.qq.com/s/DHO2CK87kI-clf4O96t5Cw

# 1. 前言

在 Raft KV 系统中，每个节点（Node）都是对等的。一个典型的请求流向是： `Client` -> `Leader Node` -> `Raft 日志同步` -> `大多数节点确认` -> `应用到状态机 (KV Store)` -> `返回 Client`。



# 2. 设计步骤

Raft 核心组件包括：一致性结点模块，RPC 通信，日志模块。

## 2.1 日志

```txt
写日志 → 复制日志 → commit → apply 【leader应用顺序】

细分一下的话就如下：
Client
   ↓
Leader
   ↓ append log (本地)
   ↓
发送 AppendEntries
   ↓
Followers append log
   ↓
多数成功
   ↓
Leader commit
   ↓
Leader apply
   ↓
返回客户端成功
   ↓
Leader 下次心跳带 commitIndex
   ↓
Followers apply
```

首先看一下，客户端发送一个请求，涉及到的大致东西有哪些：

<img src=".\images\kv1.png" style="zoom:80%;" />

从后往前看，我们需要设计的就是如何写入日志文件，以及日志文件的格式该如何设计呢？此处我们就弄简单一点儿就好了

| totalLength （int） | term（long） | index（long） | commandLength（int） | command（byte[]）          |
| ------------------- | ------------ | ------------- | -------------------- | -------------------------- |
| 整条log entry 长度  | Raft term    | 日志 index    | 命令长度             | 真正命令，这个肯定是变长的 |

totalLength = 8 (term) + 8 (index) + 4 (commandLength) + commandLength

```java
public class LogEntry {
    private final long term;
    private final long index;
    private final String command;
    ....
}
```

日志存储与管理

```java
/**
 * @Description: 日志存储与管理
 * @Author: txf
 * @Date: 2026/2/9
 */
public class LogManager {
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
}
```



## 2.2 服务端设计

### 2.2.1 消息设计

日志设计好了之后，接下来看服务端如何设计。我们使用的是netty框架，要求有netty基础。然后序列化协议采用的是protobuf，读者可以参考这篇文章：https://mp.weixin.qq.com/s/kg_-AMHRn_DzFbfBnkK4VQ 这篇文章大致讲解了一下该序列化协议，并且也是采用netty整合的。根据proto文件生成类的命令如下，也可以用idea的插件自动生成。

```proto
protoc  --proto_path=xxxx目录  --java_out=xxx目录  具体的proto文件
```

消息模板如下：

```proto
syntax = "proto3";
option java_outer_classname = "KvRaftProto"; // 生成的外层类名
option java_multiple_files = false; // 生成多个独立的Java类（而非内部类）

// ===================== 1. 通用消息封装体（核心） =====================
// Netty传输时只传这个消息，通过type识别具体消息类型
message RaftKvMessage {
  // 消息类型枚举（覆盖所有交互场景）
  enum MessageType {
    UNKNOWN = 0; // 未知类型（兜底）
    // 客户端 ↔ 节点：KV操作
    KV_REQUEST = 1;    // 客户端发起KV请求（PUT/GET/DELETE）
    KV_RESPONSE = 2;   // 节点响应客户端KV请求
    // 节点 ↔ 节点：Raft共识
    VOTE_REQUEST = 3;  // 选举请求（Candidate→Follower）
    VOTE_RESPONSE = 4; // 选举响应（Follower→Candidate）
    APPEND_ENTRIES_REQUEST = 5;  // 日志追加/心跳（Leader→Follower）
    APPEND_ENTRIES_RESPONSE = 6; // 日志追加响应（Follower→Leader）
  }

  MessageType type = 1; // 消息类型（必传）
  string node_id = 2;   // 发送方节点ID（用于识别节点）

  // 具体消息体（根据type选择其中一个）
  KvRequest kv_request = 3;
  KvResponse kv_response = 4;
  VoteRequest vote_request = 5;
  VoteResponse vote_response = 6;
  AppendEntriesRequest append_entries_request = 7;
  AppendEntriesResponse append_entries_response = 8;
}

// ===================== 2. 客户端KV操作相关 =====================
// 客户端发起的KV请求（PUT/GET/DELETE）
message KvRequest {
  enum OpType {
    PUT = 0;    // 写入/更新
    GET = 1;    // 读取
    DELETE = 2; // 删除
  }
  OpType op_type = 1; // 操作类型（必传）
  string key = 2;     // KV的key（必传）
  string value = 3;   // KV的value（仅PUT时传）
  // 可选：请求ID，用于幂等性（防止重复请求）
  string request_id = 4;
}

// 节点响应客户端的KV结果
message KvResponse {
  bool success = 1;    // 操作是否成功
  string message = 2;  // 错误信息/提示（失败时必传）
  string value = 3;    // 返回的value（仅GET成功时传）
  string request_id = 4; // 对应请求的ID（幂等性）
}

// ===================== 3. Raft选举相关 =====================
// Candidate向Follower发起的投票请求
message VoteRequest {
  int64 term = 1;                // Candidate的当前任期（必传）
  string candidate_id = 2;       // Candidate的节点ID（必传）
  int64 last_log_index = 3;      // Candidate最后一条日志的索引（用于日志一致性检查）
  int64 last_log_term = 4;       // Candidate最后一条日志的任期（用于日志一致性检查）
}

// Follower响应Candidate的投票结果
message VoteResponse {
  int64 term = 1;                // Follower的当前任期（必传，用于更新Candidate的任期）
  bool vote_granted = 2;         // 是否投赞成票（必传）
}

// ===================== 4. Raft日志追加/心跳相关 =====================
// 日志条目（与你设计的日志格式对齐，序列化后可直接写入日志文件）
message LogEntry {
  int64 term = 1;        // Raft任期（对应你日志格式的term）
  int64 index = 2;       // 日志索引（对应你日志格式的index）
  string command = 3;    // KV操作命令（如"PUT key1 value1"，对应你日志格式的command）
}

// Leader向Follower发送的日志追加/心跳请求
message AppendEntriesRequest {
  int64 term = 1;                // Leader的当前任期（必传）
  string leader_id = 2;          // Leader的节点ID（必传）
  int64 prev_log_index = 3;      // 前一条日志的索引（用于日志一致性检查）
  int64 prev_log_term = 4;       // 前一条日志的任期（用于日志一致性检查）
  repeated LogEntry entries = 5; // 待追加的日志条目（心跳时为空）
  int64 leader_commit = 6;       // Leader已提交的日志索引（Follower据此更新自己的提交索引）
}

// Follower响应Leader的日志追加结果
message AppendEntriesResponse {
  int64 term = 1;                // Follower的当前任期（必传，用于更新Leader的任期）
  bool success = 2;              // 日志追加是否成功（必传）
  int64 match_index = 3;         // Follower已匹配的日志索引（Leader据此更新nextIndex）
}
```

上面是消息的大致格式。

接下来看服务端的节点设计，我们从netty服务启动开始往下看：

在kv-core的app包里面

```java
public static void main(String[] args) {
    int port = getPort(args);
    RaftNettyServer raftNettyServer = new RaftNettyServer(port);
    try {
        raftNettyServer.start();
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
}

private static int getPort(String[] args) {
    for (String arg : args) {
        if (arg.startsWith("node.port=")) {
            return Integer.parseInt(arg.substring(10));
        }
    }
    return 7777;
}
```

从java程序启动的命令行读取结点的端口参数，我们可以用一台电脑开启多个应用，在idea中这样配置就可以了：如下图所示

<img src=".\images\kv2.png" style="zoom: 50%;" />

从上图中可以看到，配置了三个节点，然后本项目的jdk是采用的21这个版本。配置好了之后，在idea的services里面可以把这些配置一起加进去，然后就可以同时启动多个节点了。

### 2.2.2 连接设计

```java
RaftNettyServer raftNettyServer = new RaftNettyServer(port);
...
raftNettyServer.start();
```

从上一节的启动类看出来主要就是new了一个server对象，然后调用start方法。我们顺着这两个看就可以了。

首先是构造方法：

```java
public class RaftNettyServer {
  	....
    private final int port;
    private final RaftNode node;

    public RaftNettyServer(int port) {
        this.port = port;
         // 这里创建了一个raft结点对象
        this.node = new RaftNode(port);
    }
}

// 这个是RaftNode类，
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
```

Raft类是最核心的一个类。上面的构造方法其实很简单。读者自行理解。需要说明一下的就是nodeId的格式：【ip:端口】

```java
127.0.0.1:8888 // 就是这种字符串的格式
```

还有要说明的就是rpcPeers这个List的构建，可以看出来先是从配置文件读取到了集群节点列表，然后遍历这个列表创建了对象，这个具体是什么意思呢？先看一下下面的连接示意图：

<img src=".\images\kv3.png" style="zoom:50%;" />

NettyClient不仅仅是给客户用的，集群结点内部互相通信也要用到这个，rpcPeers就是集群节点内部通信使用的。如上图所示，每个节点都有一个NettyServer启动并监听着端口，同时Leader结点需要给所有的Follower结点发送心跳请求，此时这个Leader相当于其他两个Follower结点就相当于Client了，所以在结点一里面有client的部分，在项目归到了rpc的包下，也就是上面那个rpcPeers的由来了。

在集群启动的时候，都是Follower结点，只有等到超时了才会开始选主，在此之前，每个节点都有成为Candidate的可能，也就是说每个节点都有向其他结点发送投票请求的可能（VoteRequest），那么每个节点里面都要有一套Netty Client及处理流程。就如下图所示：

<img src=".\images\kv4.png" style="zoom:50%;" />

这样就有一个问题了，三个节点，我就要六个tcp连接了，四个节点就要12个连接，十个节点呢，就要90个连接，这也太多了吧，那也确实是的。有一个思路是搞一个中间层叫做routeCenter，所有结点连向它，然后消息都经过这个路由中心来转发，这样连接数就会少很多了。

还有一个思路，反正NettyClient + 一个NettyServer构建出一个Channel，理论上我只需要三个tcp连接就可以了啊。如下图所示：

<img src=".\images\kv5.png" style="zoom:50%;" />

但是这样的话，节点间通信需要转发了，代码逻辑就太复杂了，况且还有一个问题，那就是如何确定这个tcp的连接顺序呢？这个可以按照nodeId的字典顺序来嘛。反正就是麻烦就完事儿了。。。

综上所述，还是最开始的方案最简单直接了，反正这就是一个简易的案例，不要考虑太多了。如果有一些其他合适的思路，欢迎读者给出。

节点之间的连接设计就是上面的样子了。

接下来看RaftNode的设计。

### 2.2.3 RaftNode设计















# end. 参考

1. http://thinkinjava.cn/2019/01/12/2019/2019-01-12-lu-raft-kv/#%E4%BB%80%E4%B9%88%E6%98%AF-Java-%E7%89%88-Raft-%E5%88%86%E5%B8%83%E5%BC%8F-KV-%E5%AD%98%E5%82%A8
2. https://github.com/stateIs0/lu-raft-kv







