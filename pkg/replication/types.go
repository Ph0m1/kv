package replication

import "time"

// NodeState 节点状态
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry 日志条目
type LogEntry struct {
	Index   uint64 // 日志索引
	Term    uint64 // 任期号
	Command []byte // 命令数据
}

// RequestVoteRequest 投票请求
type RequestVoteRequest struct {
	Term         uint64 // 候选人的任期号
	CandidateID  string // 候选人ID
	LastLogIndex uint64 // 候选人最后日志条目的索引
	LastLogTerm  uint64 // 候选人最后日志条目的任期号
}

// RequestVoteResponse 投票响应
type RequestVoteResponse struct {
	Term        uint64 // 当前任期号
	VoteGranted bool   // 是否投票给候选人
}

// AppendEntriesRequest 追加日志请求
type AppendEntriesRequest struct {
	Term         uint64      // Leader的任期号
	LeaderID     string      // Leader的ID
	PrevLogIndex uint64      // 新日志条目之前的日志索引
	PrevLogTerm  uint64      // PrevLogIndex处的日志任期号
	Entries      []*LogEntry // 要存储的日志条目（心跳时为空）
	LeaderCommit uint64      // Leader的commitIndex
}

// AppendEntriesResponse 追加日志响应
type AppendEntriesResponse struct {
	Term    uint64 // 当前任期号
	Success bool   // 如果follower包含匹配prevLogIndex和prevLogTerm的日志，则为true
	
	// 用于快速回退的优化字段
	ConflictTerm  uint64 // 冲突条目的任期号
	ConflictIndex uint64 // 该任期的第一个索引
}

// Config Raft配置
type Config struct {
	// 节点标识
	NodeID string
	
	// 集群中的其他节点 (nodeID -> address)
	Peers map[string]string
	
	// 时间配置
	HeartbeatInterval  time.Duration // 心跳间隔
	ElectionTimeoutMin time.Duration // 选举超时最小值
	ElectionTimeoutMax time.Duration // 选举超时最大值
	
	// 快照配置
	SnapshotInterval uint64 // 快照间隔（日志条目数）
	StoragePath      string // 存储路径
}

// DefaultConfig 返回默认配置
func DefaultConfig(nodeID string) *Config {
	return &Config{
		NodeID:             nodeID,
		Peers:              make(map[string]string),
		HeartbeatInterval:  100 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		SnapshotInterval:   10000,
	}
}

// ApplyMsg 应用到状态机的消息
type ApplyMsg struct {
	CommandValid bool   // 是否是有效的命令
	Command      []byte // 命令数据
	CommandIndex uint64 // 命令索引
	
	SnapshotValid bool   // 是否是快照
	Snapshot      []byte // 快照数据
	SnapshotTerm  uint64 // 快照的最后任期
	SnapshotIndex uint64 // 快照的最后索引
}
