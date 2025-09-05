package common

import (
	"fmt"
	"os"
	"time"
)

// Config 服务器配置
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Storage     StorageConfig     `yaml:"storage"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Raft        RaftConfig        `yaml:"raft"`
	Sharding    ShardingConfig    `yaml:"sharding"`
	Logging     LoggingConfig     `yaml:"logging"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Performance PerformanceConfig `yaml:"performance"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	ListenAddr string `yaml:"listen_addr"`
	NodeID     string `yaml:"node_id"`
	DataDir    string `yaml:"data_dir"`
}

// StorageConfig 存储引擎配置
type StorageConfig struct {
	MemtableSizeLimit int64             `yaml:"memtable_size_limit"`
	SSTableSize       int64             `yaml:"sstable_size"`
	BlockSize         int               `yaml:"block_size"`
	WALSync           string            `yaml:"wal_sync"`
	Compaction        CompactionConfig  `yaml:"compaction"`
}

// CompactionConfig 压缩配置
type CompactionConfig struct {
	L0Trigger       int   `yaml:"l0_trigger"`
	BaseLevelSize   int64 `yaml:"base_level_size"`
	LevelMultiplier int   `yaml:"level_multiplier"`
	MaxLevels       int   `yaml:"max_levels"`
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	Enabled bool     `yaml:"enabled"`
	Peers   []string `yaml:"peers"`
}

// RaftConfig Raft配置
type RaftConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  int           `yaml:"snapshot_interval"`
	LogDir            string        `yaml:"log_dir"`
}

// ShardingConfig 分片配置
type ShardingConfig struct {
	Enabled           bool `yaml:"enabled"`
	VirtualNodes      int  `yaml:"virtual_nodes"`
	ReplicationFactor int  `yaml:"replication_factor"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	File       string `yaml:"file"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
}

// MetricsConfig 监控配置
type MetricsConfig struct {
	Enabled        bool   `yaml:"enabled"`
	PrometheusAddr string `yaml:"prometheus_addr"`
}

// PerformanceConfig 性能配置
type PerformanceConfig struct {
	MaxConnections   int `yaml:"max_connections"`
	ReadBufferSize   int `yaml:"read_buffer_size"`
	WriteBufferSize  int `yaml:"write_buffer_size"`
	WorkerThreads    int `yaml:"worker_threads"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr: "0.0.0.0:9090",
			NodeID:     "node-1",
			DataDir:    "./data",
		},
		Storage: StorageConfig{
			MemtableSizeLimit: 64 * 1024 * 1024, // 64MB
			SSTableSize:       64 * 1024 * 1024, // 64MB
			BlockSize:         4 * 1024,         // 4KB
			WALSync:           "always",
			Compaction: CompactionConfig{
				L0Trigger:       4,
				BaseLevelSize:   10 * 1024 * 1024, // 10MB
				LevelMultiplier: 10,
				MaxLevels:       7,
			},
		},
		Cluster: ClusterConfig{
			Enabled: false,
			Peers:   []string{},
		},
		Raft: RaftConfig{
			HeartbeatInterval: 100 * time.Millisecond,
			ElectionTimeout:   1000 * time.Millisecond,
			SnapshotInterval:  10000,
			LogDir:            "./data/raft",
		},
		Sharding: ShardingConfig{
			Enabled:           false,
			VirtualNodes:      150,
			ReplicationFactor: 3,
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "console",
			File:       "./logs/server.log",
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     7,
		},
		Metrics: MetricsConfig{
			Enabled:        true,
			PrometheusAddr: "0.0.0.0:9091",
		},
		Performance: PerformanceConfig{
			MaxConnections:  10000,
			ReadBufferSize:  4,
			WriteBufferSize: 4,
			WorkerThreads:   0, // 0表示使用CPU核心数
		},
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	// 验证服务器配置
	if c.Server.ListenAddr == "" {
		return fmt.Errorf("server.listen_addr is required")
	}
	if c.Server.NodeID == "" {
		return fmt.Errorf("server.node_id is required")
	}
	if c.Server.DataDir == "" {
		return fmt.Errorf("server.data_dir is required")
	}

	// 验证存储配置
	if c.Storage.MemtableSizeLimit <= 0 {
		return fmt.Errorf("storage.memtable_size_limit must be positive")
	}
	if c.Storage.SSTableSize <= 0 {
		return fmt.Errorf("storage.sstable_size must be positive")
	}

	// 创建必要的目录
	if err := os.MkdirAll(c.Server.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	return nil
}
