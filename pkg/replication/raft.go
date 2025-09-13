package replication

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Raft 实现Raft共识协议
type Raft struct {
	mu sync.RWMutex

	// 持久化状态（所有服务器）
	currentTerm uint64      // 当前任期号
	votedFor    string      // 当前任期投票给了谁
	log         []*LogEntry // 日志条目数组

	// 易失状态（所有服务器）
	commitIndex uint64 // 已知已提交的最高日志索引
	lastApplied uint64 // 已应用到状态机的最高日志索引

	// 易失状态（Leader）
	nextIndex  map[string]uint64 // 每个节点的下一个日志索引
	matchIndex map[string]uint64 // 每个节点已复制的最高日志索引

	// 节点状态
	state    NodeState
	leaderID string

	// 配置
	config *Config

	// 定时器
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// 应用通道
	applyCh chan ApplyMsg

	// 停止信号
	stopCh chan struct{}

	// 日志
	logger Logger

	// RPC 客户端
	rpcClient *RPCClient
}

// Logger 日志接口
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// defaultLogger 默认日志实现
type defaultLogger struct{}

func (l *defaultLogger) Debug(format string, args ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", args...)
}

func (l *defaultLogger) Info(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}

func (l *defaultLogger) Warn(format string, args ...interface{}) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}

func (l *defaultLogger) Error(format string, args ...interface{}) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}

// NewRaft 创建Raft节点
func NewRaft(config *Config, applyCh chan ApplyMsg) (*Raft, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if applyCh == nil {
		return nil, fmt.Errorf("applyCh cannot be nil")
	}

	rf := &Raft{
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		state:       Follower,
		leaderID:    "",
		config:      config,
		applyCh:     applyCh,
		stopCh:      make(chan struct{}),
		logger:      &defaultLogger{},
		rpcClient:   NewRPCClient(),
	}

	// 初始化日志（索引从1开始，0是哨兵）
	rf.log = append(rf.log, &LogEntry{Index: 0, Term: 0, Command: nil})

	return rf, nil
}

// Start 启动Raft节点
func (rf *Raft) Start() {
	rf.logger.Info("Starting Raft node %s", rf.config.NodeID)

	// 重置选举定时器
	rf.resetElectionTimer()

	// 启动应用循环
	go rf.applyLoop()

	rf.logger.Info("Raft node %s started as Follower", rf.config.NodeID)
}

// Stop 停止Raft节点
func (rf *Raft) Stop() {
	rf.logger.Info("Stopping Raft node %s", rf.config.NodeID)
	close(rf.stopCh)

	rf.mu.Lock()
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}
	rf.mu.Unlock()
}

// Submit 提交命令（只有Leader可以提交）
func (rf *Raft) Submit(command []byte) (uint64, uint64, bool, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return 0, 0, false, fmt.Errorf("not leader")
	}

	// 创建新的日志条目
	index := rf.getLastLogIndex() + 1
	entry := &LogEntry{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.config.NodeID] = index

	rf.logger.Debug("Leader %s appended log entry at index %d, term %d",
		rf.config.NodeID, index, rf.currentTerm)

	// 立即触发日志复制
	go rf.broadcastAppendEntries()

	return index, rf.currentTerm, true, nil
}

// GetState 获取当前状态
func (rf *Raft) GetState() (uint64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == Leader
}

// GetLeader 获取当前Leader
func (rf *Raft) GetLeader() string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.leaderID
}

// IsLeader 判断是否是Leader
func (rf *Raft) IsLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.state == Leader
}

// becomeFollower 转换为Follower
func (rf *Raft) becomeFollower(term uint64, leaderID string) {
	rf.logger.Info("Node %s becoming Follower (term %d -> %d)",
		rf.config.NodeID, rf.currentTerm, term)

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = ""
	rf.leaderID = leaderID

	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.resetElectionTimer()
}

// becomeCandidate 转换为Candidate
func (rf *Raft) becomeCandidate() {
	rf.logger.Info("Node %s becoming Candidate (term %d -> %d)",
		rf.config.NodeID, rf.currentTerm, rf.currentTerm+1)

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.config.NodeID
	rf.leaderID = ""

	rf.resetElectionTimer()

	// 发起选举
	go rf.startElection()
}

// becomeLeader 转换为Leader
func (rf *Raft) becomeLeader() {
	rf.logger.Info("Node %s becoming Leader (term %d)",
		rf.config.NodeID, rf.currentTerm)

	rf.state = Leader
	rf.leaderID = rf.config.NodeID

	// 初始化nextIndex和matchIndex
	lastLogIndex := rf.getLastLogIndex()
	for peerID := range rf.config.Peers {
		rf.nextIndex[peerID] = lastLogIndex + 1
		rf.matchIndex[peerID] = 0
	}
	rf.matchIndex[rf.config.NodeID] = lastLogIndex

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	// 启动心跳定时器
	rf.resetHeartbeatTimer()

	// 立即发送心跳
	go rf.broadcastAppendEntries()
}

// resetElectionTimer 重置选举定时器
func (rf *Raft) resetElectionTimer() {
	timeout := rf.randomElectionTimeout()

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	rf.electionTimer = time.AfterFunc(timeout, func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader {
			rf.becomeCandidate()
		}
	})
}

// resetHeartbeatTimer 重置心跳定时器
func (rf *Raft) resetHeartbeatTimer() {
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.heartbeatTimer = time.AfterFunc(rf.config.HeartbeatInterval, func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == Leader {
			go rf.broadcastAppendEntries()
			rf.resetHeartbeatTimer()
		}
	})
}

// randomElectionTimeout 生成随机选举超时时间
func (rf *Raft) randomElectionTimeout() time.Duration {
	min := rf.config.ElectionTimeoutMin.Milliseconds()
	max := rf.config.ElectionTimeoutMax.Milliseconds()
	timeout := min + rand.Int63n(max-min)
	return time.Duration(timeout) * time.Millisecond
}

// getLastLogIndex 获取最后一个日志索引
func (rf *Raft) getLastLogIndex() uint64 {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

// getLastLogTerm 获取最后一个日志的任期
func (rf *Raft) getLastLogTerm() uint64 {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// getLogEntry 获取指定索引的日志条目
func (rf *Raft) getLogEntry(index uint64) *LogEntry {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index {
			return rf.log[i]
		}
	}
	return nil
}

// applyLoop 应用日志到状态机
func (rf *Raft) applyLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-time.After(10 * time.Millisecond):
			rf.mu.Lock()

			// 应用已提交但未应用的日志
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				entry := rf.getLogEntry(rf.lastApplied)

				if entry != nil && entry.Command != nil {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: entry.Index,
					}

					rf.mu.Unlock()
					rf.applyCh <- msg
					rf.mu.Lock()

					rf.logger.Debug("Applied log entry at index %d", rf.lastApplied)
				}
			}

			rf.mu.Unlock()
		}
	}
}
