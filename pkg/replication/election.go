package replication

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// startElection 发起选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	
	currentTerm := rf.currentTerm
	candidateID := rf.config.NodeID
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	
	rf.logger.Info("Node %s starting election for term %d", candidateID, currentTerm)
	
	// 投票给自己
	voteCount := int32(1)
	totalPeers := len(rf.config.Peers) + 1 // 包括自己
	majority := totalPeers/2 + 1
	
	rf.mu.Unlock()
	
	// 并行向所有节点请求投票
	var wg sync.WaitGroup
	for peerID := range rf.config.Peers {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			
			req := &RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			
			resp, err := rf.sendRequestVote(nodeID, req)
			if err != nil {
				rf.logger.Debug("Failed to request vote from %s: %v", nodeID, err)
				return
			}
			
			rf.mu.Lock()
			defer rf.mu.Unlock()
			
			// 检查任期是否已经改变
			if rf.currentTerm != currentTerm || rf.state != Candidate {
				return
			}
			
			// 如果收到更高的任期，转为Follower
			if resp.Term > rf.currentTerm {
				rf.becomeFollower(resp.Term, "")
				return
			}
			
			// 统计投票
			if resp.VoteGranted {
				count := atomic.AddInt32(&voteCount, 1)
				rf.logger.Debug("Node %s received vote from %s (%d/%d)",
					candidateID, nodeID, count, totalPeers)
				
				// 获得多数票，成为Leader
				if int(count) >= majority && rf.state == Candidate {
					rf.becomeLeader()
				}
			}
		}(peerID)
	}
	
	wg.Wait()
}

// RequestVote 处理投票请求
func (rf *Raft) RequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	resp := &RequestVoteResponse{
		Term:        rf.currentTerm,
		VoteGranted: false,
	}
	
	// 如果请求的任期小于当前任期，拒绝投票
	if req.Term < rf.currentTerm {
		rf.logger.Debug("Node %s rejecting vote for %s: stale term (%d < %d)",
			rf.config.NodeID, req.CandidateID, req.Term, rf.currentTerm)
		return resp, nil
	}
	
	// 如果请求的任期大于当前任期，更新任期并转为Follower
	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term, "")
		resp.Term = rf.currentTerm
	}
	
	// 检查是否已经投票
	if rf.votedFor != "" && rf.votedFor != req.CandidateID {
		rf.logger.Debug("Node %s rejecting vote for %s: already voted for %s",
			rf.config.NodeID, req.CandidateID, rf.votedFor)
		return resp, nil
	}
	
	// 检查候选人的日志是否至少和自己一样新
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	
	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)
	
	if !logUpToDate {
		rf.logger.Debug("Node %s rejecting vote for %s: log not up-to-date",
			rf.config.NodeID, req.CandidateID)
		return resp, nil
	}
	
	// 投票给候选人
	rf.votedFor = req.CandidateID
	resp.VoteGranted = true
	
	// 重置选举定时器
	rf.resetElectionTimer()
	
	rf.logger.Info("Node %s voted for %s in term %d",
		rf.config.NodeID, req.CandidateID, rf.currentTerm)
	
	return resp, nil
}

// sendRequestVote 发送投票请求
func (rf *Raft) sendRequestVote(nodeID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	address, exists := rf.config.Peers[nodeID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found", nodeID)
	}
	
	return rf.rpcClient.RequestVote(nodeID, address, req)
}
