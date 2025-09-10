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
│   ├── replication/              # 复制层 ⏳
│   │   ├── raft.go              # Raft共识协议
│   │   ├── log.go               # 复制日志
│   │   └── snapshot.go          # 快照管理
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

### Phase 1: 重构存储引擎 
- [ ] LSM-Tree基础实现
- [ ] SkipList Memtable
- [ ] WAL持久化
- [ ] SSTable读写
- [ ] 多层级压缩

### Phase 2: 网络层
- [ ] gRPC服务定义
- [ ] 服务器实现
- [ ] 客户端SDK
- [ ] 连接池管理

### Phase 3: 分片
- [ ] 一致性哈希
- [ ] 分片路由
- [ ] 数据迁移

### Phase 4: 复制
- [ ] Raft协议
- [ ] 日志复制
- [ ] 故障恢复

### Phase 5: 集群管理
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

```bash
# 编译
make build

# 启动单节点
./bin/server -config configs/server.yaml

# 启动集群
./scripts/deploy.sh cluster

# 客户端测试
./bin/client set key1 value1
./bin/client get key1
```
