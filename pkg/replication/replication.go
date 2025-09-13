package replication

import (
	"fmt"
	"sync"
)

// broadcastAppendEntries 广播AppendEntries RPC
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderID := rf.config.NodeID
	leaderCommit := rf.commitIndex

	peers := make(map[string]string)
	for k, v := range rf.config.Peers {
		peers[k] = v
	}
	rf.mu.RUnlock()

	var wg sync.WaitGroup

	// 向每个节点发送AppendEntries
	for peerID := range peers {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			rf.sendAppendEntriesToPeer(nodeID, currentTerm, leaderID, leaderCommit)
		}(peerID)
	}

	wg.Wait()
}

// sendAppendEntriesToPeer 向单个节点发送AppendEntries
func (rf *Raft) sendAppendEntriesToPeer(peerID string, term uint64, leaderID string, leaderCommit uint64) {
	rf.mu.Lock()

	// 再次检查是否还是Leader
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := uint64(0)

	if prevLogIndex > 0 {
		prevEntry := rf.getLogEntry(prevLogIndex)
		if prevEntry != nil {
			prevLogTerm = prevEntry.Term
		}
	}

	// 准备要发送的日志条目
	entries := make([]*LogEntry, 0)
	lastLogIndex := rf.getLastLogIndex()

	if nextIndex <= lastLogIndex {
		// 需要发送日志
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Index >= nextIndex {
				entries = append([]*LogEntry{rf.log[i]}, entries...)
			}
		}
	}

	req := &AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	rf.mu.Unlock()

	// 发送RPC
	resp, err := rf.sendAppendEntries(peerID, req)
	if err != nil {
		rf.logger.Debug("Failed to send AppendEntries to %s: %v", peerID, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果收到更高的任期，转为Follower
	if resp.Term > rf.currentTerm {
		rf.becomeFollower(resp.Term, "")
		return
	}

	// 如果不再是Leader或任期已改变，忽略响应
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if resp.Success {
		// 更新nextIndex和matchIndex
		if len(entries) > 0 {
			lastEntryIndex := entries[len(entries)-1].Index
			rf.nextIndex[peerID] = lastEntryIndex + 1
			rf.matchIndex[peerID] = lastEntryIndex

			rf.logger.Debug("Successfully replicated to %s: nextIndex=%d, matchIndex=%d",
				peerID, rf.nextIndex[peerID], rf.matchIndex[peerID])

			// 尝试更新commitIndex
			rf.tryUpdateCommitIndex()
		}
	} else {
		// 日志不匹配，回退nextIndex
		if resp.ConflictIndex > 0 {
			rf.nextIndex[peerID] = resp.ConflictIndex
		} else if rf.nextIndex[peerID] > 1 {
			rf.nextIndex[peerID]--
		}

		rf.logger.Debug("Log mismatch with %s, decreasing nextIndex to %d",
			peerID, rf.nextIndex[peerID])

		// 立即重试
		go rf.sendAppendEntriesToPeer(peerID, term, leaderID, leaderCommit)
	}
}

// AppendEntries 处理AppendEntries RPC
func (rf *Raft) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:    rf.currentTerm,
		Success: false,
	}

	// 如果请求的任期小于当前任期，拒绝
	if req.Term < rf.currentTerm {
		return resp, nil
	}

	// 重置选举定时器
	rf.resetElectionTimer()

	// 如果请求的任期大于当前任期，更新任期并转为Follower
	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term, req.LeaderID)
		resp.Term = rf.currentTerm
	}

	// 如果是Candidate，转为Follower
	if rf.state == Candidate {
		rf.becomeFollower(req.Term, req.LeaderID)
	}

	// 更新Leader ID
	rf.leaderID = req.LeaderID

	// 检查prevLogIndex和prevLogTerm是否匹配
	if req.PrevLogIndex > 0 {
		prevEntry := rf.getLogEntry(req.PrevLogIndex)

		if prevEntry == nil {
			// 日志不存在
			resp.ConflictIndex = rf.getLastLogIndex() + 1
			return resp, nil
		}

		if prevEntry.Term != req.PrevLogTerm {
			// 任期不匹配
			resp.ConflictTerm = prevEntry.Term
			resp.ConflictIndex = req.PrevLogIndex

			// 找到该任期的第一个索引
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == resp.ConflictTerm {
					resp.ConflictIndex = rf.log[i].Index
					break
				}
			}

			return resp, nil
		}
	}

	// 追加新的日志条目
	if len(req.Entries) > 0 {
		// 删除冲突的日志
		for _, entry := range req.Entries {
			existingEntry := rf.getLogEntry(entry.Index)

			if existingEntry != nil && existingEntry.Term != entry.Term {
				// 删除该索引及之后的所有日志
				newLog := make([]*LogEntry, 0)
				for _, e := range rf.log {
					if e.Index < entry.Index {
						newLog = append(newLog, e)
					}
				}
				rf.log = newLog
			}
		}

		// 追加新日志
		for _, entry := range req.Entries {
			if rf.getLogEntry(entry.Index) == nil {
				rf.log = append(rf.log, entry)
			}
		}

		rf.logger.Debug("Appended %d entries from Leader %s",
			len(req.Entries), req.LeaderID)
	}

	// 更新commitIndex
	if req.LeaderCommit > rf.commitIndex {
		lastNewIndex := rf.getLastLogIndex()
		if req.LeaderCommit < lastNewIndex {
			rf.commitIndex = req.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}

		rf.logger.Debug("Updated commitIndex to %d", rf.commitIndex)
	}

	resp.Success = true
	return resp, nil
}

// tryUpdateCommitIndex 尝试更新commitIndex
func (rf *Raft) tryUpdateCommitIndex() {
	// 找到多数节点已复制的最大索引
	lastLogIndex := rf.getLastLogIndex()

	for n := lastLogIndex; n > rf.commitIndex; n-- {
		// 检查该索引的日志是否在当前任期
		entry := rf.getLogEntry(n)
		if entry == nil || entry.Term != rf.currentTerm {
			continue
		}

		// 统计有多少节点已复制该索引
		count := 1 // Leader自己
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= n {
				count++
			}
		}

		// 如果多数节点已复制，更新commitIndex
		majority := len(rf.config.Peers)/2 + 1
		if count >= majority {
			rf.commitIndex = n
			rf.logger.Info("Leader committed index %d (replicated to %d/%d nodes)",
				n, count, len(rf.config.Peers)+1)
			break
		}
	}
}

// sendAppendEntries 发送AppendEntries RPC
func (rf *Raft) sendAppendEntries(nodeID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	address, exists := rf.config.Peers[nodeID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found", nodeID)
	}
	
	return rf.rpcClient.AppendEntries(nodeID, address, req)
}
