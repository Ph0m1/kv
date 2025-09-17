package replication

import (
	"testing"
	"time"
)

// TestRaftCreation 测试 Raft 创建
func TestRaftCreation(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	if rf.config.NodeID != "node1" {
		t.Errorf("Expected node ID node1, got %s", rf.config.NodeID)
	}
	
	if rf.state != Follower {
		t.Errorf("Expected initial state Follower, got %v", rf.state)
	}
	
	if rf.currentTerm != 0 {
		t.Errorf("Expected initial term 0, got %d", rf.currentTerm)
	}
}

// TestRaftInitialState 测试初始状态
func TestRaftInitialState(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 检查初始状态
	term, isLeader := rf.GetState()
	if term != 0 {
		t.Errorf("Expected term 0, got %d", term)
	}
	if isLeader {
		t.Errorf("Expected not to be leader initially")
	}
	
	// 检查日志
	if len(rf.log) != 1 {
		t.Errorf("Expected 1 sentinel log entry, got %d", len(rf.log))
	}
}

// TestRaftSubmitNotLeader 测试非 Leader 提交
func TestRaftSubmitNotLeader(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// Follower 不能提交
	_, _, _, err = rf.Submit([]byte("command"))
	if err == nil {
		t.Errorf("Expected error when submitting to follower")
	}
}

// TestRaftRequestVote 测试投票请求
func TestRaftRequestVote(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 测试投票请求
	req := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	
	resp, err := rf.RequestVote(req)
	if err != nil {
		t.Errorf("RequestVote failed: %v", err)
	}
	
	// 应该投票给 node2
	if !resp.VoteGranted {
		t.Errorf("Expected to grant vote")
	}
	
	// 任期应该更新
	if rf.currentTerm != 1 {
		t.Errorf("Expected term 1, got %d", rf.currentTerm)
	}
	
	// 应该记录投票
	if rf.votedFor != "node2" {
		t.Errorf("Expected to vote for node2, got %s", rf.votedFor)
	}
}

// TestRaftRequestVoteStale 测试过期的投票请求
func TestRaftRequestVoteStale(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 设置当前任期为 2
	rf.currentTerm = 2
	
	// 发送任期为 1 的投票请求
	req := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	
	resp, err := rf.RequestVote(req)
	if err != nil {
		t.Errorf("RequestVote failed: %v", err)
	}
	
	// 不应该投票
	if resp.VoteGranted {
		t.Errorf("Should not grant vote for stale term")
	}
	
	// 任期不应该改变
	if rf.currentTerm != 2 {
		t.Errorf("Expected term 2, got %d", rf.currentTerm)
	}
}

// TestRaftAppendEntries 测试追加日志
func TestRaftAppendEntries(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 测试心跳
	req := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
	}
	
	resp, err := rf.AppendEntries(req)
	if err != nil {
		t.Errorf("AppendEntries failed: %v", err)
	}
	
	// 应该成功
	if !resp.Success {
		t.Errorf("Expected success")
	}
	
	// 任期应该更新
	if rf.currentTerm != 1 {
		t.Errorf("Expected term 1, got %d", rf.currentTerm)
	}
	
	// 应该成为 Follower
	if rf.state != Follower {
		t.Errorf("Expected state Follower, got %v", rf.state)
	}
	
	// Leader ID 应该更新
	if rf.leaderID != "node2" {
		t.Errorf("Expected leader node2, got %s", rf.leaderID)
	}
}

// TestRaftLogReplication 测试日志复制
func TestRaftLogReplication(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 追加日志条目
	entries := []*LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
	}
	
	req := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	}
	
	resp, err := rf.AppendEntries(req)
	if err != nil {
		t.Errorf("AppendEntries failed: %v", err)
	}
	
	if !resp.Success {
		t.Errorf("Expected success")
	}
	
	// 验证日志
	if len(rf.log) != 3 { // sentinel + 2 entries
		t.Errorf("Expected 3 log entries, got %d", len(rf.log))
	}
}

// TestCommandEncoding 测试命令编解码
func TestCommandEncoding(t *testing.T) {
	// 测试 Put 命令
	cmd := EncodeCommand(CommandPut, []byte("key"), []byte("value"))
	decoded, err := DecodeCommand(cmd)
	if err != nil {
		t.Errorf("DecodeCommand failed: %v", err)
	}
	
	if decoded.Type != CommandPut {
		t.Errorf("Expected CommandPut, got %v", decoded.Type)
	}
	if string(decoded.Key) != "key" {
		t.Errorf("Expected key, got %s", string(decoded.Key))
	}
	if string(decoded.Value) != "value" {
		t.Errorf("Expected value, got %s", string(decoded.Value))
	}
	
	// 测试 Delete 命令
	cmd = EncodeCommand(CommandDelete, []byte("key"), nil)
	decoded, err = DecodeCommand(cmd)
	if err != nil {
		t.Errorf("DecodeCommand failed: %v", err)
	}
	
	if decoded.Type != CommandDelete {
		t.Errorf("Expected CommandDelete, got %v", decoded.Type)
	}
	if string(decoded.Key) != "key" {
		t.Errorf("Expected key, got %s", string(decoded.Key))
	}
}

// TestRaftStateTransition 测试状态转换
func TestRaftStateTransition(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 初始状态应该是 Follower
	if rf.state != Follower {
		t.Errorf("Expected initial state Follower")
	}
	
	// 转换为 Candidate
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	
	if rf.state != Candidate {
		t.Errorf("Expected state Candidate")
	}
	if rf.currentTerm != 1 {
		t.Errorf("Expected term 1")
	}
	if rf.votedFor != "node1" {
		t.Errorf("Expected to vote for self")
	}
	
	// 转换为 Leader
	rf.mu.Lock()
	rf.becomeLeader()
	rf.mu.Unlock()
	
	if rf.state != Leader {
		t.Errorf("Expected state Leader")
	}
	if rf.leaderID != "node1" {
		t.Errorf("Expected to be leader")
	}
}

// BenchmarkRaftSubmit 性能测试：提交命令
func BenchmarkRaftSubmit(b *testing.B) {
	applyCh := make(chan ApplyMsg, 1000)
	config := DefaultConfig("node1")
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		b.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	// 强制成为 Leader
	rf.mu.Lock()
	rf.becomeLeader()
	rf.mu.Unlock()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := []byte("command")
		rf.Submit(cmd)
	}
}

// TestRaftElectionTimeout 测试选举超时
func TestRaftElectionTimeout(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	config := DefaultConfig("node1")
	config.ElectionTimeoutMin = 100 * time.Millisecond
	config.ElectionTimeoutMax = 200 * time.Millisecond
	
	rf, err := NewRaft(config, applyCh)
	if err != nil {
		t.Fatalf("Failed to create Raft: %v", err)
	}
	defer rf.Stop()
	
	rf.Start()
	
	// 等待选举超时
	time.Sleep(300 * time.Millisecond)
	
	// 应该变成 Candidate
	rf.mu.RLock()
	state := rf.state
	rf.mu.RUnlock()
	
	if state != Candidate {
		t.Errorf("Expected state Candidate after election timeout, got %v", state)
	}
}
