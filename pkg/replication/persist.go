package replication

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// PersistentState 持久化状态
type PersistentState struct {
	CurrentTerm       uint64      `json:"current_term"`
	VotedFor          string      `json:"voted_for"`
	Log               []*LogEntry `json:"log"`
	LastSnapshotIndex uint64      `json:"last_snapshot_index"`
	LastSnapshotTerm  uint64      `json:"last_snapshot_term"`
}

// persist 持久化 Raft 状态
func (rf *Raft) persist() error {
	if rf.config.StoragePath == "" {
		return nil // 不持久化
	}
	
	state := PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastSnapshotIndex: rf.lastSnapshotIndex,
		LastSnapshotTerm:  rf.lastSnapshotTerm,
	}
	
	// 创建存储目录
	if err := os.MkdirAll(rf.config.StoragePath, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}
	
	// 写入临时文件
	statePath := filepath.Join(rf.config.StoragePath, "state.json")
	tmpPath := statePath + ".tmp"
	
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create state file: %w", err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(&state); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode state: %w", err)
	}
	
	if err := file.Sync(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync state: %w", err)
	}
	
	// 原子性重命名
	if err := os.Rename(tmpPath, statePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename state file: %w", err)
	}
	
	return nil
}

// loadPersistentState 加载持久化状态
func (rf *Raft) loadPersistentState() error {
	if rf.config.StoragePath == "" {
		return nil // 不加载
	}
	
	statePath := filepath.Join(rf.config.StoragePath, "state.json")
	file, err := os.Open(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，使用默认值
		}
		return fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()
	
	var state PersistentState
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}
	
	// 恢复状态
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.lastSnapshotIndex = state.LastSnapshotIndex
	rf.lastSnapshotTerm = state.LastSnapshotTerm
	
	return nil
}
