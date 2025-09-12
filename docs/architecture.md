# 系统架构文档

## 概述

这是一个基于 LSM-Tree 的分布式键值存储系统，提供高性能的读写能力和强一致性保证。

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Layer                  │
│                  (gRPC Client / SDK)              │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Server Layer                   │
│              (gRPC Server / Request Handler)      │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Storage Engine Layer               │
│                    (LSM-Tree Engine)                 │
│  ┌────────────┐  ┌────────────┐  ┌──────────┐  ┌────────────┐ │
│  │ Memtable │  │   WAL    │  │ SSTable│  │Compaction│ │
│  └────────────┘  └────────────┘  └──────────┘  └────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. 存储引擎层 (Storage Engine)

基于 LSM-Tree 的存储引擎，提供高性能的写入和读取能力。

#### 组件

**Memtable (内存表)**
- 实现：SkipList（跳表）
- 功能：缓存最新的写入数据
- 大小限制：64MB（可配置）
- 特点：有序存储，O(log N) 查找

**WAL (Write-Ahead Log)**
- 功能：保证数据持久性
- 格式：二进制日志
- 恢复：启动时自动回放
- 清理：Memtable 刷盘后删除

**SSTable (Sorted String Table)**
- 格式：不可变的有序文件
- 结构：
  ```
  [Data Block 1] [Data Block 2] ... [Index Block] [Footer]
  ```
- Block 大小：4KB（可配置）
- 压缩：支持 Snappy 压缩（可选）

**Compaction (压缩)**
- 策略：分层压缩（Leveled Compaction）
- 层级：L0 - L6（7层）
- 触发：L0 达到 4 个文件时
- 算法：多路归并排序 + 去重

#### 数据流

**写入流程**：
```
1. 写入 WAL（持久化）
2. 写入 Memtable（内存）
3. Memtable 满时刷盘到 L0
4. 后台压缩到 L1-L6
```

**读取流程**：
```
1. 查找 Active Memtable
2. 查找 Immutable Memtable
3. 查找 L0 SSTables（从新到旧）
4. 查找 L1-L6 SSTables（二分查找）
```

#### 层级结构

| 层级 | 大小限制 | 文件数 | 特点 |
|------|---------|--------|------|
| L0 | 无限制 | 4+ | 键可重叠 |
| L1 | 10MB | 多个 | 键不重叠 |
| L2 | 100MB | 多个 | 键不重叠 |
| L3 | 1GB | 多个 | 键不重叠 |
| L4 | 10GB | 多个 | 键不重叠 |
| L5 | 100GB | 多个 | 键不重叠 |
| L6 | 1TB | 多个 | 键不重叠 |

### 2. 网络层 (Network Layer)

基于 gRPC 的高性能网络通信。

#### 服务器 (Server)

**功能**：
- 接收客户端请求
- 路由到存储引擎
- 返回响应

**特性**：
- Keep-Alive 连接管理
- 请求日志记录
- 错误处理
- 优雅关闭

**接口**：
```protobuf
service KVService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc BatchGet(BatchGetRequest) returns (BatchGetResponse);
  rpc BatchPut(BatchPutRequest) returns (BatchPutResponse);
}
```

#### 客户端 (Client)

**功能**：
- 连接管理
- 请求发送
- 响应处理

**特性**：
- 自动重连
- 超时控制
- 批量操作
- 连接池（待实现）

### 3. 通用模块 (Common)

**配置管理 (Config)**
- YAML 配置文件
- 默认配置
- 配置验证

**日志 (Logger)**
- 分级日志（DEBUG, INFO, WARN, ERROR）
- 文件输出
- 控制台输出

**错误处理 (Errors)**
- 统一错误定义
- 错误码
- 错误消息

## 数据模型

### 键值对

```go
type Entry struct {
    Key         []byte  // 键
    Value       []byte  // 值
    IsTombstone bool    // 是否为墓碑（删除标记）
}
```

### 删除语义

- 删除操作写入墓碑标记
- 墓碑在压缩时被物理删除
- 读取时墓碑视为不存在

## 性能特性

### 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| Put | O(log N) | SkipList 插入 |
| Get | O(log N × L) | L = 层级数 |
| Delete | O(log N) | 写入墓碑 |
| Scan | O(K) | K = 扫描的键数 |

### 空间复杂度

| 组件 | 空间 | 说明 |
|------|------|------|
| Memtable | 64MB | 可配置 |
| WAL | ~64MB | 与 Memtable 对应 |
| SSTables | 无限制 | 受磁盘限制 |

### 吞吐量

- **写入**：~100K ops/s（单机）
- **读取**：~50K ops/s（单机）
- **压缩**：后台异步，不阻塞读写

## 一致性保证

### 写入一致性

- WAL 保证持久性
- Memtable 保证可见性
- 压缩保证最终一致性

### 读取一致性

- 读取最新写入的数据
- 快照隔离（Snapshot Isolation）

## 故障恢复

### 崩溃恢复

1. 启动时回放 WAL
2. 重建 Memtable
3. 加载 SSTables
4. 继续服务

### 数据完整性

- WAL Checksum 校验
- SSTable Checksum 校验
- 损坏文件自动跳过

## 配置参数

### 存储引擎

```yaml
storage:
  memtable_size_limit: 67108864  # 64MB
  sstable_size: 67108864         # 64MB
  block_size: 4096               # 4KB
  compaction:
    l0_trigger: 4
    base_level_size: 10485760    # 10MB
    level_multiplier: 10
    max_levels: 7
```

### 服务器

```yaml
server:
  listen_addr: "0.0.0.0:9090"
  node_id: "node-1"
  data_dir: "./data"
```

### 性能

```yaml
performance:
  max_connections: 10000
  read_buffer_size: 4
  write_buffer_size: 4
  worker_threads: 0  # 0 = CPU 核心数
```

## 监控指标

### 关键指标

1. **写入指标**
   - 写入 QPS
   - 写入延迟（P50, P99）
   - WAL 写入速度

2. **读取指标**
   - 读取 QPS
   - 读取延迟（P50, P99）
   - 缓存命中率

3. **压缩指标**
   - 压缩次数
   - 压缩延迟
   - 磁盘使用率

4. **系统指标**
   - CPU 使用率
   - 内存使用率
   - 磁盘 I/O

## 扩展性

### 当前支持

- ✅ 单机高性能存储
- ✅ gRPC 网络通信
- ✅ 批量操作

### 未来扩展

- ⏳ 数据分片（Sharding）
- ⏳ 数据复制（Replication）
- ⏳ 集群管理（Cluster）
- ⏳ 分布式事务

## 最佳实践

### 1. 键设计

```
推荐：
- user:1001:name
- order:2023:11:05:001
- cache:session:abc123

避免：
- 过长的键（> 1KB）
- 随机键（影响压缩效率）
```

### 2. 值大小

```
推荐：
- 小值（< 1MB）
- 固定大小的值

避免：
- 大值（> 10MB）
- 存储二进制大文件
```

### 3. 批量操作

```go
// 推荐：使用批量操作
items := map[string][]byte{
    "key1": []byte("value1"),
    "key2": []byte("value2"),
}
client.BatchPut(ctx, items)

// 避免：循环单次操作
for k, v := range items {
    client.Put(ctx, []byte(k), v)
}
```

### 4. 压缩策略

```
- 定期监控 L0 文件数
- 避免频繁的大量写入
- 预留足够的磁盘空间
```

## 参考资料

- [LSM-Tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [LevelDB Design](https://github.com/google/leveldb/blob/master/doc/impl.md)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [gRPC Documentation](https://grpc.io/docs/)
