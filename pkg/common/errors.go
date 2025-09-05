package common

import "errors"

// 通用错误定义
var (
	// ErrKeyNotFound 键不存在
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyDeleted 键已被删除
	ErrKeyDeleted = errors.New("key deleted")

	// ErrInvalidKey 无效的键
	ErrInvalidKey = errors.New("invalid key")

	// ErrInvalidValue 无效的值
	ErrInvalidValue = errors.New("invalid value")

	// ErrStorageClosed 存储引擎已关闭
	ErrStorageClosed = errors.New("storage closed")

	// ErrWALCorrupted WAL文件损坏
	ErrWALCorrupted = errors.New("wal corrupted")

	// ErrSSTableCorrupted SSTable文件损坏
	ErrSSTableCorrupted = errors.New("sstable corrupted")

	// ErrCompactionFailed 压缩失败
	ErrCompactionFailed = errors.New("compaction failed")
)

// 集群相关错误
var (
	// ErrNotLeader 当前节点不是Leader
	ErrNotLeader = errors.New("not leader")

	// ErrNoLeader 集群中没有Leader
	ErrNoLeader = errors.New("no leader")

	// ErrNodeNotFound 节点不存在
	ErrNodeNotFound = errors.New("node not found")

	// ErrClusterNotReady 集群未就绪
	ErrClusterNotReady = errors.New("cluster not ready")

	// ErrReplicationFailed 复制失败
	ErrReplicationFailed = errors.New("replication failed")
)

// 分片相关错误
var (
	// ErrShardNotFound 分片不存在
	ErrShardNotFound = errors.New("shard not found")

	// ErrWrongShard 键不属于当前分片
	ErrWrongShard = errors.New("wrong shard")

	// ErrMigrationInProgress 数据迁移进行中
	ErrMigrationInProgress = errors.New("migration in progress")
)

// 网络相关错误
var (
	// ErrTimeout 操作超时
	ErrTimeout = errors.New("operation timeout")

	// ErrConnectionClosed 连接已关闭
	ErrConnectionClosed = errors.New("connection closed")

	// ErrTooManyConnections 连接数过多
	ErrTooManyConnections = errors.New("too many connections")
)
