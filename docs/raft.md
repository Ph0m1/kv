# Raft 共识协议实现

## 概述

Raft 是一个用于管理复制日志的共识算法，提供强一致性保证和自动故障转移能力。

## 核心概念

### 1. 节点状态

Raft 节点有三种状态：

- **Follower（跟随者）**
  - 被动接收来自 Leader 的日志
  - 响应投票请求
  - 如果超时未收到心跳，转为 Candidate

- **Candidate（候选者）**
  - 发起选举
  - 请求其他节点投票
  - 获得多数票后成为 Leader

- **Leader（领导者）**
  - 接收客户端请求
  - 复制日志到 Followers
  - 发送心跳维持权威

### 2. 任期（Term）

```
Term 1    Term 2    Term 3    Term 4
  │         │         │         │
  ├─Leader──┤         │         │
  │         ├─Election┤         │
  │         │         ├─Leader──┤
```

- 时间被划分为任期
- 每个任期最多一个 Leader
- 某些任期可能没有 Leader（选举失败）
- 任期号单调递增

### 3. 日志结构

```
Index:  1    2    3    4    5    6
Term:   1    1    1    2    3    3
Cmd:   [x←1][y←2][x←3][y←4][x←5][y←6]
```

## 核心算法

### 1. Leader 选举

#### 触发条件
- Follower 超时未收到 Leader 心跳
- Candidate 选举超时

#### 选举流程

```
1. Follower 超时
   ↓
2. 转为 Candidate，任期+1
   ↓
3. 投票给自己
   ↓
4. 并行请求其他节点投票
   ↓
5. 等待结果：
   - 获得多数票 → 成为 Leader
   - 收到 Leader 心跳 → 转为 Follower
   - 超时 → 重新选举
```

#### 投票规则

节点投票给候选者需要满足：
1. 候选者任期 >= 自己的任期
2. 本任期还未投票，或已投票给该候选者
3. 候选者日志至少和自己一样新

**日志新旧判断**：
```go
candidateLogUpToDate := 
    candidateLastTerm > myLastTerm ||
    (candidateLastTerm == myLastTerm && 
     candidateLastIndex >= myLastIndex)
```

### 2. 日志复制

#### 复制流程

```
Leader:
1. 接收客户端命令
2. 追加到本地日志
3. 并行发送 AppendEntries RPC
4. 等待多数派确认
5. 提交日志（更新 commitIndex）
6. 应用到状态机
7. 返回客户端

Follower:
1. 接收 AppendEntries RPC
2. 检查 prevLogIndex 和 prevLogTerm
3. 如果匹配，追加新日志
4. 更新 commitIndex
5. 应用已提交的日志
```

#### 日志一致性检查

```
Leader 日志:  [1,1] [2,1] [3,2] [4,2]
                              ↑
                         prevLogIndex=3
                         prevLogTerm=2

Follower 日志: [1,1] [2,1] [3,2] [?]
                              ↑
                         检查是否匹配
```

如果匹配，追加新日志；否则，Leader 回退 nextIndex 重试。

### 3. 安全性保证

#### Leader 完整性

**规则**：如果某个日志条目在某个任期被提交，那么这个条目必然出现在更高任期的所有 Leader 的日志中。

**保证方式**：
- 选举限制：只有拥有最新日志的节点才能当选
- 提交规则：Leader 只提交当前任期的日志

#### 状态机安全性

**规则**：如果某个节点已将某个日志条目应用到状态机，那么其他节点在相同索引处不会应用不同的日志。

## API 使用

### 创建 Raft 节点

```go
// 创建配置
config := &Config{
    NodeID:             "node1",
    Peers:              map[string]string{
        "node2": "192.168.1.2:9090",
        "node3": "192.168.1.3:9090",
    },
    HeartbeatInterval:  100 * time.Millisecond,
    ElectionTimeoutMin: 150 * time.Millisecond,
    ElectionTimeoutMax: 300 * time.Millisecond,
}

// 创建应用通道
applyCh := make(chan ApplyMsg, 100)

// 创建 Raft 节点
rf, err := NewRaft(config, applyCh)
if err != nil {
    log.Fatal(err)
}

// 启动节点
rf.Start()
```

### 提交命令

```go
// 只有 Leader 可以提交命令
if rf.IsLeader() {
    command := []byte("SET key value")
    index, term, isLeader, err := rf.Submit(command)
    
    if err != nil {
        log.Printf("Submit failed: %v", err)
    } else {
        log.Printf("Command submitted: index=%d, term=%d", index, term)
    }
}
```

### 应用日志

```go
// 监听应用通道
go func() {
    for msg := range applyCh {
        if msg.CommandValid {
            // 应用命令到状态机
            applyCommand(msg.Command)
            log.Printf("Applied command at index %d", msg.CommandIndex)
        }
    }
}()
```

## 配置参数

### 时间参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| HeartbeatInterval | 100ms | 心跳间隔 |
| ElectionTimeoutMin | 150ms | 选举超时最小值 |
| ElectionTimeoutMax | 300ms | 选举超时最大值 |

**推荐设置**：
- 心跳间隔 < 选举超时 / 10
- 选举超时范围：150ms - 300ms（局域网）
- 选举超时范围：1s - 3s（广域网）

## 性能特性

### 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| Submit | O(1) | 追加到日志 |
| AppendEntries | O(N) | N = 日志条目数 |
| RequestVote | O(1) | 投票决策 |

### 吞吐量

- **写入吞吐量**：受网络延迟和磁盘 I/O 限制
- **读取吞吐量**：Leader 可以直接响应，无需共识

## 故障场景

### 1. Leader 故障

```
Before:
  Leader: node1
  Followers: node2, node3

After:
  1. node2, node3 选举超时
  2. node2 或 node3 成为新 Leader
  3. 客户端重定向到新 Leader
```

**恢复时间**：1-2 个选举超时周期（~300-600ms）

### 2. 网络分区

#### 场景 A：多数派可用

```
Partition:
  [node1, node2] | [node3]
  
Result:
  - node1 或 node2 继续作为 Leader
  - node3 无法当选（无法获得多数票）
  - 系统继续服务
```

#### 场景 B：无多数派

```
Partition:
  [node1] | [node2] | [node3]
  
Result:
  - 无节点能获得多数票
  - 系统停止服务（保证一致性）
  - 分区恢复后重新选举
```

## 最佳实践

### 1. 集群大小

```
推荐配置：
- 3 节点：容忍 1 个故障
- 5 节点：容忍 2 个故障
- 7 节点：容忍 3 个故障

不推荐：
- 偶数节点（浪费资源）
- 超过 7 个节点（性能下降）
```

### 2. 客户端重试

```go
func submitWithRetry(rf *Raft, cmd []byte) error {
    maxRetries := 3
    
    for i := 0; i < maxRetries; i++ {
        if rf.IsLeader() {
            _, _, _, err := rf.Submit(cmd)
            if err == nil {
                return nil
            }
        }
        
        // 等待并重定向到新 Leader
        time.Sleep(100 * time.Millisecond)
        leaderID := rf.GetLeader()
        // 连接到新 Leader
    }
    
    return fmt.Errorf("failed after %d retries", maxRetries)
}
```

## 与存储引擎集成

```go
// 创建 Raft 节点
applyCh := make(chan ApplyMsg, 100)
rf, _ := NewRaft(config, applyCh)
rf.Start()

// 创建存储引擎
store, _ := storage.NewLSM("./data")

// 应用 Raft 日志到存储引擎
go func() {
    for msg := range applyCh {
        if !msg.CommandValid {
            continue
        }
        
        // 解析命令
        key, value := parseCommand(msg.Command)
        store.Put(key, value)
    }
}()

// 客户端写入
func Put(key, value []byte) error {
    if !rf.IsLeader() {
        return fmt.Errorf("not leader")
    }
    
    cmd := encodeCommand(key, value)
    _, _, _, err := rf.Submit(cmd)
    return err
}
```

## 参考

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)
- [etcd Raft](https://github.com/etcd-io/raft)
