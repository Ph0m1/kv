package replication

import (
	"context"

	"github.com/ph0m1/kv/pkg/proto"
)

// RPCServer 实现 RaftService gRPC 服务
type RPCServer struct {
	proto.UnimplementedRaftServiceServer
	raft *Raft
}

// NewRPCServer 创建 RPC 服务端
func NewRPCServer(raft *Raft) *RPCServer {
	return &RPCServer{
		raft: raft,
	}
}

// RequestVote 处理投票请求
func (s *RPCServer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	raftReq := &RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	
	raftResp, err := s.raft.RequestVote(raftReq)
	if err != nil {
		return nil, err
	}
	
	return &proto.RequestVoteResponse{
		Term:        raftResp.Term,
		VoteGranted: raftResp.VoteGranted,
	}, nil
}

// AppendEntries 处理追加日志请求
func (s *RPCServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	// 转换日志条目
	entries := make([]*LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		}
	}
	
	raftReq := &AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
	
	raftResp, err := s.raft.AppendEntries(raftReq)
	if err != nil {
		return nil, err
	}
	
	return &proto.AppendEntriesResponse{
		Term:          raftResp.Term,
		Success:       raftResp.Success,
		ConflictTerm:  raftResp.ConflictTerm,
		ConflictIndex: raftResp.ConflictIndex,
	}, nil
}
