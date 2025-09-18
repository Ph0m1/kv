# Distributed KV Storage

基于LSM-Tree的分布式键值存储系统

## 项目结构

```
kv/
├── cmd/                          # 可执行程序入口
│   ├── server/                   # KV服务器
│   │   └── main.go
│   └── client/                   # 命令行客户端
│       └── main.go
│
├── pkg/                          # 公共库
│   ├── storage/                  # 存储引擎层 (LSM-Tree) ✅
│   │   ├── lsm.go               # LSM引擎主逻辑
│   │   ├── memtable.go          # Memtable (SkipList)
│   │   ├── wal.go               # Write-Ahead Log
│   │   ├── compactor_heap.go    # 压缩堆
│   │   ├── level_manager.go     # 多层级管理
│   │   ├── interface.go         # 存储接口
│   │   └── sstable/             # SSTable实现
│   │       ├── reader.go
│   │       ├── writer.go
│   │       ├── iterator.go
│   │       └── interface.go
│   │
│   ├── server/                   # 服务器层 ✅
│   │   ├── server.go            # gRPC服务器
│   │   └── interceptor.go       # 拦截器
│   │
│   ├── client/                   # 客户端层 ✅
│   │   └── client.go            # gRPC客户端
│   │
│   ├── sharding/                 # 分片层 ✅
│   │   ├── hash.go              # 一致性哈希
│   │   └── manager.go           # 分片管理器
│   │
│   ├── common/                   # 通用模块 ✅
│   │   ├── config.go            # 配置管理
│   │   ├── errors.go            # 错误定义
│   │   └── logger.go            # 日志工具
│   │
│   ├── proto/                    # Protocol Buffers ✅
│   │   ├── kv.proto             # 接口定义
│   │   ├── kv.pb.go             # 生成的代码
│   │   └── kv_grpc.pb.go        # 生成的gRPC代码
│   │
│   ├── cluster/                  # 集群管理层 ⏳
│   │   ├── node.go              # 节点管理
│   │   ├── membership.go        # 成员管理
│   │   └── discovery.go         # 服务发现
│   │
│   ├── replication/              # 复制层 ✅
│   │   ├── raft.go              # Raft核心结构和接口
│   │   ├── election.go          # Leader选举逻辑
│   │   ├── replication.go       # 日志复制逻辑
│   │   ├── types.go             # 类型定义
│   │   ├── rpc_server.go        # Raft RPC服务端
│   │   ├── rpc_client.go        # Raft RPC客户端
│   │   ├── command.go           # 命令编解码
│   │   └── snapshot.go          # 快照管理 (待实现)
│   │
│   ├── proto/                    # Protocol Buffers定义
│   │   ├── kv.proto             # KV服务接口
│   │   ├── raft.proto           # Raft协议
│   │   └── cluster.proto        # 集群管理
│   │
│   └── common/                   # 通用工具
│       ├── config.go            # 配置管理
│       ├── logger.go            # 日志
│       ├── errors.go            # 错误定义
│       └── utils.go             # 工具函数
│
├── internal/                     # 内部实现（不对外暴露）
│   └── metrics/                 # 监控指标
│       └── metrics.go
│
├── configs/                      # 配置文件
│   ├── server.yaml              # 服务器配置
│   └── cluster.yaml             # 集群配置
│
├── scripts/                      # 脚本
│   ├── build.sh                 # 编译脚本
│   └── deploy.sh                # 部署脚本
│
├── test/                         # 测试
│   ├── integration/             # 集成测试
│   └── benchmark/               # 性能测试
│
├── docs/                         # 文档
│   ├── architecture.md          # 架构设计
│   ├── api.md                   # API文档
│   └── deployment.md            # 部署文档
│
├── go.mod                        # Go模块定义
├── go.sum                        # 依赖锁定
├── Makefile                      # 构建工具
└── README.md                     # 项目说明
```

## 架构层次

### 1. 存储引擎层 (Storage Engine)
- LSM-Tree实现
- Memtable (SkipList)
- WAL (Write-Ahead Log)
- SSTable (Sorted String Table)
- Compaction (压缩)

### 2. 服务器层 (Server)
- gRPC服务
- 请求处理
- 连接管理

### 3. 集群管理层 (Cluster)
- 节点发现
- 成员管理
- 健康检查

### 4. 复制层 (Replication)
- Raft共识协议
- 日志复制
- 快照管理

### 5. 分片层 (Sharding)
- 一致性哈希
- 数据分片
- 请求路由

## 开发路线

### Phase 1: 存储引擎 ✅ 已完成
- [x] LSM-Tree基础实现
- [x] SkipList Memtable
- [x] WAL持久化
- [x] SSTable读写
- [x] 多层级压缩

### Phase 2: 网络层 ✅ 已完成
- [x] gRPC服务定义
- [x] 服务器实现
- [x] 客户端SDK
- [x] 批量操作

### Phase 3: 分片 ✅ 已完成
- [x] 一致性哈希
- [x] 分片管理器
- [x] 请求路由

### Phase 4: 复制 ✅ 已完成
- [x] Raft协议核心实现
- [x] Leader选举
- [x] 日志复制
- [x] RPC服务端/客户端
- [x] 命令编解码
- [x] 集成到KV服务器
- [ ] 快照管理 (待实现)
- [ ] 配置变更 (待实现)

### Phase 5: 集群管理 ⏳ 待实现
- [ ] 服务发现
- [ ] 配置管理
- [ ] 监控告警

## 技术栈

- **语言**: Go 1.25+
- **RPC**: gRPC
- **共识**: Raft
- **序列化**: Protocol Buffers
- **日志**: Zap
- **配置**: Viper

## 快速开始

### 单节点模式

```bash
# 编译
make build

# 启动单节点服务器
./bin/server --config configs/server.yaml

# 客户端测试
./bin/client set key1 value1
./bin/client get key1
```

### Raft 集群模式

```bash
# 生成 protobuf 代码
make proto

# 启动 3 节点 Raft 集群
# 节点 1
./bin/server --config configs/node1.yaml --raft

# 节点 2
./bin/server --config configs/node2.yaml --raft

# 节点 3
./bin/server --config configs/node3.yaml --raft

# 客户端连接到 Leader
./bin/client --addr localhost:8001 set key1 value1
./bin/client --addr localhost:8001 get key1
```

详细使用说明请参考：
- [Raft 使用指南](docs/raft_usage.md)
- [存储引擎文档](docs/storage.md)
- [测试文档](docs/testing.md)
