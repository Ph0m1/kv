package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ph0m1/kv/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RPCClient Raft RPC 客户端
type RPCClient struct {
	mu      sync.RWMutex
	clients map[string]proto.RaftServiceClient
	conns   map[string]*grpc.ClientConn
}

// NewRPCClient 创建 RPC 客户端
func NewRPCClient() *RPCClient {
	return &RPCClient{
		clients: make(map[string]proto.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

// getClient 获取或创建到指定节点的客户端
func (c *RPCClient) getClient(nodeID, address string) (proto.RaftServiceClient, error) {
	c.mu.RLock()
	client, exists := c.clients[nodeID]
	c.mu.RUnlock()
	
	if exists {
		return client, nil
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// 双重检查
	if client, exists := c.clients[nodeID]; exists {
		return client, nil
	}
	
	// 创建新连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}
	
	client = proto.NewRaftServiceClient(conn)
	c.clients[nodeID] = client
	c.conns[nodeID] = conn
	
	return client, nil
}

// RequestVote 发送投票请求
func (c *RPCClient) RequestVote(nodeID, address string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	client, err := c.getClient(nodeID, address)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	
	pbReq := &proto.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	
	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	
	return &RequestVoteResponse{
		Term:        pbResp.Term,
		VoteGranted: pbResp.VoteGranted,
	}, nil
}

// AppendEntries 发送追加日志请求
func (c *RPCClient) AppendEntries(nodeID, address string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	client, err := c.getClient(nodeID, address)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	
	// 转换日志条目
	pbEntries := make([]*proto.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		pbEntries[i] = &proto.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
	}
	
	pbReq := &proto.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	}
	
	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	
	return &AppendEntriesResponse{
		Term:          pbResp.Term,
		Success:       pbResp.Success,
		ConflictTerm:  pbResp.ConflictTerm,
		ConflictIndex: pbResp.ConflictIndex,
	}, nil
}

// Close 关闭所有连接
func (c *RPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, conn := range c.conns {
		conn.Close()
	}
	
	c.clients = make(map[string]proto.RaftServiceClient)
	c.conns = make(map[string]*grpc.ClientConn)
	
	return nil
}
