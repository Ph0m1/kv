package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ph0m1/kv/pkg/client"
	"github.com/ph0m1/kv/pkg/common"
	"github.com/ph0m1/kv/pkg/replication"
	"github.com/ph0m1/kv/pkg/server"
	"github.com/ph0m1/kv/pkg/storage"
)

// TestSingleNodeIntegration 单节点集成测试
func TestSingleNodeIntegration(t *testing.T) {
	// 创建临时目录
	tmpDir := createTempDir(t)
	defer os.RemoveAll(tmpDir)

	// 创建配置
	config := &common.Config{
		Server: common.ServerConfig{
			Address: "localhost:18001",
		},
		Storage: common.StorageConfig{
			DataDir: tmpDir,
		},
	}

	// 创建存储引擎
	lsm, err := storage.NewLSM(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()

	// 创建服务器
	srv, err := server.NewServer(config, lsm)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer srv.Stop()

	// 等待服务器启动
	time.Sleep(500 * time.Millisecond)

	// 创建客户端
	cli, err := client.NewClient("localhost:18001")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 测试 Put
	err = cli.Put(ctx, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 测试 Get
	value, err := cli.Get(ctx, []byte("key1"))
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}

	// 测试 Delete
	err = cli.Delete(ctx, []byte("key1"))
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// 验证删除
	_, err = cli.Get(ctx, []byte("key1"))
	if err == nil {
		t.Errorf("Expected error for deleted key")
	}
}

// TestRaftClusterIntegration Raft 集群集成测试
func TestRaftClusterIntegration(t *testing.T) {
	// 跳过长时间运行的测试
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// 创建 3 个节点的集群
	nodes := make([]*testNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = createTestNode(t, i+1)
		defer nodes[i].cleanup()
	}

	// 启动所有节点
	for _, node := range nodes {
		go func(n *testNode) {
			if err := n.server.Start(); err != nil {
				t.Logf("Server %s error: %v", n.nodeID, err)
			}
		}(node)
	}

	// 等待集群稳定
	time.Sleep(2 * time.Second)

	// 找到 Leader
	var leaderNode *testNode
	for _, node := range nodes {
		if node.raft != nil {
			term, isLeader := node.raft.GetState()
			if isLeader {
				leaderNode = node
				t.Logf("Found leader: %s (term=%d)", node.nodeID, term)
				break
			}
		}
	}

	if leaderNode == nil {
		t.Fatal("No leader elected")
	}

	// 连接到 Leader
	cli, err := client.NewClient(leaderNode.address)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 测试写入
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := cli.Put(ctx, key, value)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}

	// 等待复制
	time.Sleep(1 * time.Second)

	// 验证数据在所有节点上
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expected := fmt.Sprintf("value%d", i)

		for _, node := range nodes {
			value, found, err := node.storage.Get(key)
			if err != nil {
				t.Errorf("Get failed on node %s: %v", node.nodeID, err)
			}
			if !found {
				t.Errorf("Key not found on node %s", node.nodeID)
			}
			if string(value) != expected {
				t.Errorf("Node %s: expected %s, got %s", node.nodeID, expected, string(value))
			}
		}
	}
}

// TestFailoverIntegration 故障转移集成测试
func TestFailoverIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// 创建 3 个节点
	nodes := make([]*testNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = createTestNode(t, i+1)
		defer nodes[i].cleanup()
	}

	// 启动所有节点
	for _, node := range nodes {
		go func(n *testNode) {
			n.server.Start()
		}(node)
	}

	// 等待集群稳定
	time.Sleep(2 * time.Second)

	// 找到 Leader
	var leaderIdx int
	for i, node := range nodes {
		if node.raft != nil {
			_, isLeader := node.raft.GetState()
			if isLeader {
				leaderIdx = i
				break
			}
		}
	}

	// 停止 Leader
	t.Logf("Stopping leader node %d", leaderIdx+1)
	nodes[leaderIdx].server.Stop()

	// 等待新 Leader 选举
	time.Sleep(2 * time.Second)

	// 验证新 Leader 被选出
	newLeaderFound := false
	for i, node := range nodes {
		if i == leaderIdx {
			continue
		}
		if node.raft != nil {
			_, isLeader := node.raft.GetState()
			if isLeader {
				newLeaderFound = true
				t.Logf("New leader elected: node %d", i+1)
				break
			}
		}
	}

	if !newLeaderFound {
		t.Error("No new leader elected after failover")
	}
}

// testNode 测试节点
type testNode struct {
	nodeID  string
	address string
	dataDir string
	storage *storage.LSM
	raft    *replication.Raft
	server  *server.RaftServer
}

// createTestNode 创建测试节点
func createTestNode(t *testing.T, id int) *testNode {
	nodeID := fmt.Sprintf("node%d", id)
	address := fmt.Sprintf("localhost:1800%d", id)
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("kv-test-%d-%d", id, time.Now().UnixNano()))

	// 创建存储
	lsm, err := storage.NewLSM(dataDir)
	if err != nil {
		t.Fatalf("Failed to create LSM for %s: %v", nodeID, err)
	}

	// 创建配置
	config := &common.Config{
		Server: common.ServerConfig{
			Address:    address,
			NodeID:     nodeID,
			ListenAddr: address,
		},
		Storage: common.StorageConfig{
			DataDir: dataDir,
		},
		Cluster: common.ClusterConfig{
			NodeID:  nodeID,
			Enabled: true,
			Peers: []string{
				"node1=localhost:19001",
				"node2=localhost:19002",
				"node3=localhost:19003",
			},
		},
	}

	// 创建 Raft 服务器 (它会内部创建 Raft 实例)
	srv, err := server.NewRaftServer(config, lsm)
	if err != nil {
		t.Fatalf("Failed to create Raft server for %s: %v", nodeID, err)
	}

	return &testNode{
		nodeID:  nodeID,
		address: address,
		dataDir: dataDir,
		storage: lsm,
		raft:    srv.GetRaft(),
		server:  srv,
	}
}

// cleanup 清理测试节点
func (n *testNode) cleanup() {
	if n.server != nil {
		n.server.Stop()
	}
	if n.raft != nil {
		n.raft.Stop()
	}
	if n.storage != nil {
		n.storage.Close()
	}
	os.RemoveAll(n.dataDir)
}

// createTempDir 创建临时目录
func createTempDir(t *testing.T) string {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("kv-test-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}
