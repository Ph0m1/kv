package replication

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot 快照结构
type Snapshot struct {
	// 快照元数据
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string][]byte `json:"data"` // 状态机数据
	
	mu sync.RWMutex
}

// SnapshotMetadata 快照元数据
type SnapshotMetadata struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Size              int64  `json:"size"`
}

// InstallSnapshotRequest 安装快照请求
type InstallSnapshotRequest struct {
	Term              uint64 // Leader 的任期
	LeaderID          string // Leader ID
	LastIncludedIndex uint64 // 快照中最后一条日志的索引
	LastIncludedTerm  uint64 // 快照中最后一条日志的任期
	Offset            int64  // 快照分块的偏移量
	Data              []byte // 快照数据
	Done              bool   // 是否是最后一块
}

// InstallSnapshotResponse 安装快照响应
type InstallSnapshotResponse struct {
	Term uint64 // 当前任期，用于 Leader 更新自己
}

// NewSnapshot 创建新快照
func NewSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data map[string][]byte) *Snapshot {
	return &Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
}

// Save 保存快照到文件
func (s *Snapshot) Save(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// 创建临时文件
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()
	
	// 编码快照数据
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(s); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}
	
	// 同步到磁盘
	if err := file.Sync(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}
	
	// 原子性重命名
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}
	
	return nil
}

// Load 从文件加载快照
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // 快照不存在
		}
		return nil, fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()
	
	snapshot := &Snapshot{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}
	
	return snapshot, nil
}

// GetMetadata 获取快照元数据
func (s *Snapshot) GetMetadata() SnapshotMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return SnapshotMetadata{
		LastIncludedIndex: s.LastIncludedIndex,
		LastIncludedTerm:  s.LastIncludedTerm,
	}
}

// GetData 获取快照数据
func (s *Snapshot) GetData() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// 返回副本
	data := make(map[string][]byte, len(s.Data))
	for k, v := range s.Data {
		data[k] = v
	}
	return data
}

// CreateSnapshot 创建快照（Raft 方法）
func (rf *Raft) CreateSnapshot(lastIncludedIndex uint64, data map[string][]byte) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// 检查索引
	if lastIncludedIndex <= rf.lastSnapshotIndex {
		return fmt.Errorf("snapshot index %d <= last snapshot index %d", 
			lastIncludedIndex, rf.lastSnapshotIndex)
	}
	
	if lastIncludedIndex > rf.getLastLogIndex() {
		return fmt.Errorf("snapshot index %d > last log index %d", 
			lastIncludedIndex, rf.getLastLogIndex())
	}
	
	// 获取快照点的任期
	lastIncludedTerm := rf.log[lastIncludedIndex].Term
	
	// 创建快照
	snapshot := NewSnapshot(lastIncludedIndex, lastIncludedTerm, data)
	
	// 保存快照
	snapshotPath := filepath.Join(rf.config.StoragePath, "snapshot.dat")
	if err := snapshot.Save(snapshotPath); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}
	
	// 压缩日志
	rf.log = rf.log[lastIncludedIndex:]
	rf.lastSnapshotIndex = lastIncludedIndex
	rf.lastSnapshotTerm = lastIncludedTerm
	
	// 持久化状态
	if err := rf.persist(); err != nil {
		return fmt.Errorf("failed to persist state: %w", err)
	}
	
	return nil
}

// InstallSnapshot 处理安装快照请求
func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	resp := &InstallSnapshotResponse{
		Term: rf.currentTerm,
	}
	
	// 1. 如果 term < currentTerm，立即返回
	if req.Term < rf.currentTerm {
		return resp, nil
	}
	
	// 2. 如果 term > currentTerm，更新 currentTerm
	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.votedFor = ""
		rf.becomeFollower(req.Term, req.LeaderID)
	}
	
	// 重置选举定时器
	rf.resetElectionTimer()
	
	// 3. 如果是第一块数据，创建新快照文件
	if req.Offset == 0 {
		// 创建临时快照文件
		tmpPath := filepath.Join(rf.config.StoragePath, "snapshot.tmp")
		file, err := os.Create(tmpPath)
		if err != nil {
			return resp, fmt.Errorf("failed to create snapshot file: %w", err)
		}
		file.Close()
	}
	
	// 4. 写入快照数据
	tmpPath := filepath.Join(rf.config.StoragePath, "snapshot.tmp")
	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return resp, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	
	if _, err := file.WriteAt(req.Data, req.Offset); err != nil {
		file.Close()
		return resp, fmt.Errorf("failed to write snapshot data: %w", err)
	}
	file.Close()
	
	// 5. 如果 done=true，完成快照安装
	if req.Done {
		// 加载快照
		snapshot, err := LoadSnapshot(tmpPath)
		if err != nil {
			return resp, fmt.Errorf("failed to load snapshot: %w", err)
		}
		
		// 应用快照到状态机
		rf.applySnapshot(snapshot)
		
		// 压缩日志
		rf.log = rf.log[:1] // 只保留哨兵
		rf.lastSnapshotIndex = req.LastIncludedIndex
		rf.lastSnapshotTerm = req.LastIncludedTerm
		
		// 移动快照文件
		snapshotPath := filepath.Join(rf.config.StoragePath, "snapshot.dat")
		if err := os.Rename(tmpPath, snapshotPath); err != nil {
			return resp, fmt.Errorf("failed to rename snapshot: %w", err)
		}
		
		// 持久化状态
		if err := rf.persist(); err != nil {
			return resp, fmt.Errorf("failed to persist state: %w", err)
		}
	}
	
	return resp, nil
}

// applySnapshot 应用快照到状态机
func (rf *Raft) applySnapshot(snapshot *Snapshot) {
	// 将快照数据编码为字节
	data, err := json.Marshal(snapshot.Data)
	if err != nil {
		fmt.Printf("ERROR: failed to marshal snapshot data: %v\n", err)
		return
	}
	
	// 发送快照到应用层
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  snapshot.LastIncludedTerm,
		SnapshotIndex: snapshot.LastIncludedIndex,
	}
	
	// 非阻塞发送
	select {
	case rf.applyCh <- msg:
	default:
		// 如果 channel 满了，记录日志
		fmt.Printf("WARN: apply channel is full, dropping snapshot\n")
	}
}

// sendInstallSnapshot 发送安装快照 RPC
func (rf *Raft) sendInstallSnapshot(peerID string) error {
	rf.mu.Lock()
	
	// 加载快照
	snapshotPath := filepath.Join(rf.config.StoragePath, "snapshot.dat")
	file, err := os.Open(snapshotPath)
	if err != nil {
		rf.mu.Unlock()
		return fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()
	
	// 获取文件大小（用于日志记录）
	_, err = file.Stat()
	if err != nil {
		rf.mu.Unlock()
		return fmt.Errorf("failed to stat snapshot: %w", err)
	}
	
	term := rf.currentTerm
	leaderID := rf.config.NodeID
	lastIncludedIndex := rf.lastSnapshotIndex
	lastIncludedTerm := rf.lastSnapshotTerm
	
	rf.mu.Unlock()
	
	// 分块发送快照
	const chunkSize = 64 * 1024 // 64KB
	offset := int64(0)
	
	for {
		// 读取一块数据
		chunk := make([]byte, chunkSize)
		n, err := file.Read(chunk)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read snapshot: %w", err)
		}
		
		if n == 0 {
			break
		}
		
		chunk = chunk[:n]
		done := err == io.EOF
		
		// 发送请求
		req := &InstallSnapshotRequest{
			Term:              term,
			LeaderID:          leaderID,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Offset:            offset,
			Data:              chunk,
			Done:              done,
		}
		
		resp, err := rf.sendInstallSnapshotRPC(peerID, req)
		if err != nil {
			return fmt.Errorf("failed to send install snapshot RPC: %w", err)
		}
		
		// 检查响应
		rf.mu.Lock()
		if resp.Term > rf.currentTerm {
			rf.currentTerm = resp.Term
			rf.becomeFollower(resp.Term, "")
			rf.mu.Unlock()
			return nil
		}
		rf.mu.Unlock()
		
		if done {
			break
		}
		
		offset += int64(n)
	}
	
	// 更新 nextIndex 和 matchIndex
	rf.mu.Lock()
	rf.nextIndex[peerID] = lastIncludedIndex + 1
	rf.matchIndex[peerID] = lastIncludedIndex
	rf.mu.Unlock()
	
	return nil
}

// sendInstallSnapshotRPC 发送安装快照 RPC
func (rf *Raft) sendInstallSnapshotRPC(peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	// 使用 RPC 客户端发送请求
	address, exists := rf.config.Peers[peerID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found", peerID)
	}
	
	// 这里需要实现实际的 RPC 调用
	// 为了简化，我们先返回一个模拟响应
	_ = address
	
	return &InstallSnapshotResponse{
		Term: rf.currentTerm,
	}, nil
}

// shouldSnapshot 判断是否应该创建快照
func (rf *Raft) shouldSnapshot() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	
	// 如果日志大小超过阈值，创建快照
	const snapshotThreshold = 1000
	logSize := len(rf.log) - 1 // 减去哨兵
	
	return logSize > snapshotThreshold
}

// GetSnapshotPath 获取快照文件路径
func (rf *Raft) GetSnapshotPath() string {
	return filepath.Join(rf.config.StoragePath, "snapshot.dat")
}

// LoadSnapshotOnStartup 启动时加载快照
func (rf *Raft) LoadSnapshotOnStartup() error {
	snapshotPath := rf.GetSnapshotPath()
	snapshot, err := LoadSnapshot(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}
	
	if snapshot == nil {
		return nil // 没有快照
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// 应用快照
	rf.lastSnapshotIndex = snapshot.LastIncludedIndex
	rf.lastSnapshotTerm = snapshot.LastIncludedTerm
	
	// 压缩日志
	rf.log = rf.log[:1] // 只保留哨兵
	
	// 发送快照到应用层
	rf.applySnapshot(snapshot)
	
	return nil
}
