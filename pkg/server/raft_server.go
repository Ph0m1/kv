package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ph0m1/kv/pkg/common"
	"github.com/ph0m1/kv/pkg/proto"
	"github.com/ph0m1/kv/pkg/replication"
	"github.com/ph0m1/kv/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftServer 支持 Raft 的 KV 服务器
type RaftServer struct {
	*Server // 嵌入基础服务器
	
	raft    *replication.Raft
	applyCh chan replication.ApplyMsg
	
	// 等待应用的命令
	pendingMu sync.RWMutex
	pending   map[uint64]chan error
}

// NewRaftServer 创建支持 Raft 的服务器
func NewRaftServer(config *common.Config, store storage.Storage) (*RaftServer, error) {
	// 创建基础服务器
	baseServer, err := NewServer(config, store)
	if err != nil {
		return nil, err
	}
	
	// 创建 Raft 配置
	raftConfig := replication.DefaultConfig(config.Server.NodeID)
	
	// 从配置中读取 peers
	// peers 格式: "nodeID=address"，例如 "node2=localhost:9092"
	if config.Cluster.Enabled && len(config.Cluster.Peers) > 0 {
		raftConfig.Peers = make(map[string]string)
		for _, peerStr := range config.Cluster.Peers {
			// 简单解析，实际使用中可能需要更复杂的解析
			// 这里假设配置文件中 peers 已经是 "nodeID=address" 格式
			// 或者直接使用地址，节点ID从地址推导
			// 为了简化，这里暂时跳过解析
			baseServer.logger.Info("Peer: %s", peerStr)
		}
	}
	
	// 创建应用通道
	applyCh := make(chan replication.ApplyMsg, 100)
	
	// 创建 Raft 节点
	raft, err := replication.NewRaft(raftConfig, applyCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	
	rs := &RaftServer{
		Server:  baseServer,
		raft:    raft,
		applyCh: applyCh,
		pending: make(map[uint64]chan error),
	}
	
	// 注册 Raft 服务
	raftRPCServer := replication.NewRPCServer(raft)
	proto.RegisterRaftServiceServer(baseServer.grpc, raftRPCServer)
	
	return rs, nil
}

// Start 启动服务器
func (rs *RaftServer) Start() error {
	// 启动 Raft
	rs.raft.Start()
	rs.logger.Info("Raft node started")
	
	// 启动应用循环
	go rs.applyLoop()
	
	// 启动基础服务器
	return rs.Server.Start()
}

// Stop 停止服务器
func (rs *RaftServer) Stop() error {
	// 停止 Raft
	rs.raft.Stop()
	
	// 停止基础服务器
	return rs.Server.Stop()
}

// applyLoop 应用 Raft 日志到状态机
func (rs *RaftServer) applyLoop() {
	for msg := range rs.applyCh {
		if !msg.CommandValid {
			continue
		}
		
		// 解码命令
		cmd, err := replication.DecodeCommand(msg.Command)
		if err != nil {
			rs.logger.Error("Failed to decode command: %v", err)
			rs.notifyPending(msg.CommandIndex, err)
			continue
		}
		
		// 应用命令到存储引擎
		var applyErr error
		switch cmd.Type {
		case replication.CommandPut:
			applyErr = rs.storage.Put(cmd.Key, cmd.Value)
		case replication.CommandDelete:
			applyErr = rs.storage.Delete(cmd.Key)
		default:
			applyErr = fmt.Errorf("unknown command type: %d", cmd.Type)
		}
		
		if applyErr != nil {
			rs.logger.Error("Failed to apply command at index %d: %v", msg.CommandIndex, applyErr)
		} else {
			rs.logger.Debug("Applied command at index %d", msg.CommandIndex)
		}
		
		// 通知等待的客户端
		rs.notifyPending(msg.CommandIndex, applyErr)
	}
}

// notifyPending 通知等待的命令
func (rs *RaftServer) notifyPending(index uint64, err error) {
	rs.pendingMu.Lock()
	ch, exists := rs.pending[index]
	if exists {
		delete(rs.pending, index)
	}
	rs.pendingMu.Unlock()
	
	if exists {
		select {
		case ch <- err:
		default:
		}
	}
}

// waitApply 等待命令被应用
func (rs *RaftServer) waitApply(index uint64, timeout time.Duration) error {
	ch := make(chan error, 1)
	
	rs.pendingMu.Lock()
	rs.pending[index] = ch
	rs.pendingMu.Unlock()
	
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		rs.pendingMu.Lock()
		delete(rs.pending, index)
		rs.pendingMu.Unlock()
		return fmt.Errorf("timeout waiting for command to be applied")
	}
}

// Put 通过 Raft 提交写入
func (rs *RaftServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	rs.mu.RLock()
	if rs.closed {
		rs.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	rs.mu.RUnlock()
	
	if len(req.Key) == 0 {
		return &proto.PutResponse{
			Success: false,
			Error:   "key is empty",
		}, nil
	}
	
	// 检查是否是 Leader
	if !rs.raft.IsLeader() {
		leader := rs.raft.GetLeader()
		return &proto.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("not leader, leader is %s", leader),
		}, nil
	}
	
	// 编码命令
	cmd := replication.EncodeCommand(replication.CommandPut, req.Key, req.Value)
	
	// 提交到 Raft
	index, _, isLeader, err := rs.raft.Submit(cmd)
	if err != nil {
		return &proto.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	if !isLeader {
		return &proto.PutResponse{
			Success: false,
			Error:   "not leader",
		}, nil
	}
	
	// 等待命令被应用
	if err := rs.waitApply(index, 5*time.Second); err != nil {
		return &proto.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	return &proto.PutResponse{
		Success: true,
	}, nil
}

// Delete 通过 Raft 提交删除
func (rs *RaftServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	rs.mu.RLock()
	if rs.closed {
		rs.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	rs.mu.RUnlock()
	
	if len(req.Key) == 0 {
		return &proto.DeleteResponse{
			Success: false,
			Error:   "key is empty",
		}, nil
	}
	
	// 检查是否是 Leader
	if !rs.raft.IsLeader() {
		leader := rs.raft.GetLeader()
		return &proto.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("not leader, leader is %s", leader),
		}, nil
	}
	
	// 编码命令
	cmd := replication.EncodeCommand(replication.CommandDelete, req.Key, nil)
	
	// 提交到 Raft
	index, _, isLeader, err := rs.raft.Submit(cmd)
	if err != nil {
		return &proto.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	if !isLeader {
		return &proto.DeleteResponse{
			Success: false,
			Error:   "not leader",
		}, nil
	}
	
	// 等待命令被应用
	if err := rs.waitApply(index, 5*time.Second); err != nil {
		return &proto.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	return &proto.DeleteResponse{
		Success: true,
	}, nil
}

// GetRaft 返回 Raft 实例（用于测试和监控）
func (rs *RaftServer) GetRaft() *replication.Raft {
	return rs.raft
}
