# 分片层文档

## 概述

分片层实现了数据的水平分片，使用一致性哈希算法将数据分布到多个节点上，支持动态扩缩容。

## 架构

```
┌─────────────────────────────────────────────────────┐
│                   Client Request                     │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│                  Sharding Manager                    │
│  - 一致性哈希环                                       │
│  - 节点管理                                          │
│  - 请求路由                                          │
└─────────────────────────────────────────────────────┘
                         │
                ┌────────┼────────┐
                ▼        ▼        ▼
         ┌──────────┐ ┌──────────┐ ┌──────────┐
         │  Node 1  │ │  Node 2  │ │  Node 3  │
         │ Storage  │ │ Storage  │ │ Storage  │
         └──────────┘ └──────────┘ └──────────┘
```

## 核心组件

### 1. 一致性哈希 (Consistent Hash)

#### 原理

一致性哈希将节点和键都映射到一个哈希环上（0 到 2^32-1）：

```
         0
         │
    node3#5
         │
    node1#8
         │
    node2#12
         │
        ...
         │
    2^32-1
```

#### 虚拟节点

每个物理节点映射到多个虚拟节点（默认150个）：

```
node1 → node1#0, node1#1, ..., node1#149
node2 → node2#0, node2#1, ..., node2#149
node3 → node3#0, node3#1, ..., node3#149
```

**优点**：
- 负载均衡更好
- 减少数据迁移量

#### API

```go
// 创建哈希环
ch := NewConsistentHash(150)

// 添加节点
ch.Add("node1")
ch.Add("node2")
ch.Add("node3")

// 获取键对应的节点
node := ch.Get("mykey")

// 获取N个副本节点
nodes := ch.GetN("mykey", 3)

// 移除节点
ch.Remove("node1")
```

### 2. 分片管理器 (Sharding Manager)

#### 功能

- 管理集群中的所有节点
- 维护节点到客户端的连接
- 路由请求到正确的节点
- 支持本地和远程操作

#### 创建管理器

```go
// 创建分片管理器
manager := NewManager(
    "node1",        // 本地节点ID
    localStorage,   // 本地存储引擎
    3,              // 副本因子
    150,            // 虚拟节点数
)

// 添加节点
manager.AddNode("node1", "")                      // 本地节点
manager.AddNode("node2", "192.168.1.2:9090")     // 远程节点
manager.AddNode("node3", "192.168.1.3:9090")     // 远程节点
```

#### 路由请求

```go
ctx := context.Background()

// Get操作（自动路由）
value, found, err := manager.Get(ctx, []byte("key"))

// Put操作（自动路由）
err = manager.Put(ctx, []byte("key"), []byte("value"))

// Delete操作（自动路由）
err = manager.Delete(ctx, []byte("key"))
```

## 数据分布

### 键的映射

```
key1 → hash(key1) = 1234 → 查找环上第一个 >= 1234 的虚拟节点 → node1
key2 → hash(key2) = 5678 → 查找环上第一个 >= 5678 的虚拟节点 → node2
key3 → hash(key3) = 9999 → 查找环上第一个 >= 9999 的虚拟节点 → node3
```

### 负载均衡

使用150个虚拟节点时，负载分布：

| 节点数 | 标准差 | 最大偏差 |
|--------|--------|----------|
| 3 | ~3% | ~10% |
| 5 | ~2% | ~8% |
| 10 | ~1% | ~5% |

## 扩缩容

### 添加节点

```go
// 添加新节点
manager.AddNode("node4", "192.168.1.4:9090")
```

**影响**：
- 只有约 1/N 的数据需要迁移（N = 节点总数）
- 其他数据保持不变

**示例**（3节点 → 4节点）：
```
Before: key1→node1, key2→node2, key3→node3
After:  key1→node1, key2→node4, key3→node3
                      ↑ 只有 key2 迁移
```

### 删除节点

```go
// 移除节点
manager.RemoveNode("node2")
```

**影响**：
- 该节点的数据迁移到相邻节点
- 其他数据保持不变

## 使用示例

### 示例1：单节点模式

```go
// 创建本地存储
storage, _ := storage.NewLSM("./data")

// 创建分片管理器（单节点）
manager := NewManager("node1", storage, 1, 150)
manager.AddNode("node1", "")

// 所有请求都在本地处理
ctx := context.Background()
manager.Put(ctx, []byte("key"), []byte("value"))
value, found, _ := manager.Get(ctx, []byte("key"))
```

### 示例2：三节点集群

```go
// 节点1
storage1, _ := storage.NewLSM("./data1")
manager1 := NewManager("node1", storage1, 3, 150)
manager1.AddNode("node1", "")
manager1.AddNode("node2", "192.168.1.2:9090")
manager1.AddNode("node3", "192.168.1.3:9090")

// 节点2
storage2, _ := storage.NewLSM("./data2")
manager2 := NewManager("node2", storage2, 3, 150)
manager2.AddNode("node1", "192.168.1.1:9090")
manager2.AddNode("node2", "")
manager2.AddNode("node3", "192.168.1.3:9090")

// 节点3
storage3, _ := storage.NewLSM("./data3")
manager3 := NewManager("node3", storage3, 3, 150)
manager3.AddNode("node1", "192.168.1.1:9090")
manager3.AddNode("node2", "192.168.1.2:9090")
manager3.AddNode("node3", "")

// 在任意节点上操作
ctx := context.Background()
manager1.Put(ctx, []byte("key1"), []byte("value1"))  // 可能路由到node2
manager2.Put(ctx, []byte("key2"), []byte("value2"))  // 可能路由到node3
manager3.Get(ctx, []byte("key1"))                     // 自动路由到正确节点
```

### 示例3：判断键的位置

```go
// 判断键是否在本地
isLocal, _ := manager.IsLocalKey("mykey")
if isLocal {
    // 本地处理
} else {
    // 远程处理
}

// 获取键对应的节点
node, _ := manager.GetNode("mykey")
fmt.Printf("Key belongs to: %s\n", node.ID)
```

## 性能特性

### 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| Add | O(V log V) | V = 虚拟节点数 |
| Remove | O(V) | 需要过滤虚拟节点 |
| Get | O(log V) | 二分查找 |
| GetN | O(V) | 最坏情况遍历所有虚拟节点 |

### 空间复杂度

| 数据结构 | 空间 | 说明 |
|----------|------|------|
| 哈希环 | O(N × V) | N = 物理节点数 |
| 节点映射 | O(N) | 节点信息 |
| 客户端连接 | O(N) | 每个远程节点一个连接 |

## 配置参数

### 虚拟节点数

```go
// 小集群 (< 10节点)
manager := NewManager(nodeID, storage, 3, 50)

// 中等集群 (10-100节点)
manager := NewManager(nodeID, storage, 3, 150)  // 推荐

// 大集群 (> 100节点)
manager := NewManager(nodeID, storage, 3, 500)
```

### 副本因子

```go
// 开发环境
replicationFactor := 1

// 生产环境
replicationFactor := 3  // 推荐

// 高可用环境
replicationFactor := 5
```

## 故障处理

### 节点故障

```go
// 检测到节点故障
if !isNodeHealthy("node2") {
    // 从集群中移除
    manager.RemoveNode("node2")
    
    // 数据会自动路由到其他节点
}
```

### 网络分区

- 使用多数派保证一致性
- 多数派可用时系统继续服务
- 少数派拒绝写入

## 监控指标

### 关键指标

1. **节点分布**
   - 每个节点的键数量
   - 负载均衡程度

2. **请求路由**
   - 本地请求比例
   - 远程请求比例
   - 路由延迟

3. **扩缩容**
   - 数据迁移进度
   - 迁移速率

## 最佳实践

### 1. 节点命名

```go
// 使用有意义的节点ID
manager.AddNode("dc1-rack1-node1", "10.0.1.1:9090")
manager.AddNode("dc1-rack2-node2", "10.0.1.2:9090")
manager.AddNode("dc2-rack1-node3", "10.0.2.1:9090")
```

### 2. 虚拟节点数量

```
推荐配置：
- 3-10 节点：50-100 虚拟节点
- 10-100 节点：150 虚拟节点
- 100+ 节点：300-500 虚拟节点
```

### 3. 副本策略

```
推荐配置：
- 开发：1 副本
- 测试：2 副本
- 生产：3 副本
- 关键数据：5 副本
```

### 4. 渐进式扩容

```go
// 不推荐：一次添加多个节点
manager.AddNode("node4", "...")
manager.AddNode("node5", "...")
manager.AddNode("node6", "...")

// 推荐：逐个添加，观察负载
manager.AddNode("node4", "...")
// 等待数据迁移完成
time.Sleep(time.Minute)
manager.AddNode("node5", "...")
```

## 未来扩展

- [ ] 数据迁移工具
- [ ] 副本同步
- [ ] 读修复
- [ ] 自动故障转移
- [ ] 监控仪表板

## 参考

- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Cassandra Architecture](https://cassandra.apache.org/doc/latest/architecture/)
