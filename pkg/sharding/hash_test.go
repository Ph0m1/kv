package sharding

import (
	"fmt"
	"testing"
)

// TestConsistentHashBasic 测试基本操作
func TestConsistentHashBasic(t *testing.T) {
	ch := NewConsistentHash(10)
	
	// 添加节点
	ch.Add("node1")
	ch.Add("node2")
	ch.Add("node3")
	
	// 检查节点数量
	if ch.Count() != 3 {
		t.Errorf("Expected 3 nodes, got %d", ch.Count())
	}
	
	// 获取键对应的节点
	node := ch.Get("key1")
	if node == "" {
		t.Errorf("Expected non-empty node")
	}
	
	// 同一个键应该总是映射到同一个节点
	node2 := ch.Get("key1")
	if node != node2 {
		t.Errorf("Expected consistent mapping")
	}
}

// TestConsistentHashRemove 测试删除节点
func TestConsistentHashRemove(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node1")
	ch.Add("node2")
	ch.Add("node3")
	
	// 删除节点
	ch.Remove("node2")
	
	if ch.Count() != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", ch.Count())
	}
	
	// 确保不会映射到已删除的节点
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		node := ch.Get(key)
		if node == "node2" {
			t.Errorf("Should not map to removed node")
		}
	}
}

// TestConsistentHashDistribution 测试负载分布
func TestConsistentHashDistribution(t *testing.T) {
	ch := NewConsistentHash(150)
	
	ch.Add("node1")
	ch.Add("node2")
	ch.Add("node3")
	
	// 统计分布
	distribution := make(map[string]int)
	totalKeys := 10000
	
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		node := ch.Get(key)
		distribution[node]++
	}
	
	// 检查每个节点的负载
	expectedPerNode := totalKeys / 3
	tolerance := expectedPerNode / 5 // 20% 容差
	
	for node, count := range distribution {
		diff := count - expectedPerNode
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("Node %s has unbalanced load: %d (expected ~%d)", 
				node, count, expectedPerNode)
		}
	}
}

// TestConsistentHashGetN 测试获取多个副本节点
func TestConsistentHashGetN(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node1")
	ch.Add("node2")
	ch.Add("node3")
	
	// 获取 3 个副本节点
	nodes := ch.GetN("key1", 3)
	
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}
	
	// 检查是否有重复
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node] {
			t.Errorf("Duplicate node in replica list: %s", node)
		}
		seen[node] = true
	}
}

// TestConsistentHashGetNMoreThanAvailable 测试请求超过可用节点数
func TestConsistentHashGetNMoreThanAvailable(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node1")
	ch.Add("node2")
	
	// 请求 5 个节点，但只有 2 个可用
	nodes := ch.GetN("key1", 5)
	
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes (all available), got %d", len(nodes))
	}
}

// TestConsistentHashEmpty 测试空哈希环
func TestConsistentHashEmpty(t *testing.T) {
	ch := NewConsistentHash(10)
	
	// 空环应该返回空字符串
	node := ch.Get("key1")
	if node != "" {
		t.Errorf("Expected empty string for empty ring")
	}
	
	// GetN 应该返回空列表
	nodes := ch.GetN("key1", 3)
	if len(nodes) != 0 {
		t.Errorf("Expected empty list for empty ring")
	}
}

// TestConsistentHashAddDuplicate 测试添加重复节点
func TestConsistentHashAddDuplicate(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node1")
	ch.Add("node1") // 重复添加
	
	// 应该只有一个节点
	if ch.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", ch.Count())
	}
}

// TestConsistentHashRemoveNonexistent 测试删除不存在的节点
func TestConsistentHashRemoveNonexistent(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node1")
	ch.Remove("node2") // 删除不存在的节点
	
	// 应该不影响现有节点
	if ch.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", ch.Count())
	}
}

// TestConsistentHashNodes 测试获取所有节点
func TestConsistentHashNodes(t *testing.T) {
	ch := NewConsistentHash(10)
	
	ch.Add("node3")
	ch.Add("node1")
	ch.Add("node2")
	
	nodes := ch.Nodes()
	
	// 应该返回排序后的节点列表
	expected := []string{"node1", "node2", "node3"}
	if len(nodes) != len(expected) {
		t.Errorf("Expected %d nodes, got %d", len(expected), len(nodes))
	}
	
	for i, node := range nodes {
		if node != expected[i] {
			t.Errorf("Expected node %s at position %d, got %s", 
				expected[i], i, node)
		}
	}
}

// TestConsistentHashMinimalMigration 测试最小化数据迁移
func TestConsistentHashMinimalMigration(t *testing.T) {
	ch := NewConsistentHash(150)
	
	// 初始 3 个节点
	ch.Add("node1")
	ch.Add("node2")
	ch.Add("node3")
	
	// 记录初始映射
	initialMapping := make(map[string]string)
	totalKeys := 1000
	
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		node := ch.Get(key)
		initialMapping[key] = node
	}
	
	// 添加第 4 个节点
	ch.Add("node4")
	
	// 统计改变的映射
	changed := 0
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		newNode := ch.Get(key)
		if initialMapping[key] != newNode {
			changed++
		}
	}
	
	// 理论上应该迁移约 1/4 的数据
	expectedChange := totalKeys / 4
	tolerance := expectedChange / 2 // 50% 容差
	
	diff := changed - expectedChange
	if diff < 0 {
		diff = -diff
	}
	
	if diff > tolerance {
		t.Errorf("Too many keys changed: %d (expected ~%d)", 
			changed, expectedChange)
	}
}

// BenchmarkConsistentHashAdd 性能测试：添加节点
func BenchmarkConsistentHashAdd(b *testing.B) {
	ch := NewConsistentHash(150)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := fmt.Sprintf("node%d", i)
		ch.Add(node)
	}
}

// BenchmarkConsistentHashGet 性能测试：查找
func BenchmarkConsistentHashGet(b *testing.B) {
	ch := NewConsistentHash(150)
	
	// 添加节点
	for i := 0; i < 10; i++ {
		ch.Add(fmt.Sprintf("node%d", i))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		ch.Get(key)
	}
}

// BenchmarkConsistentHashGetN 性能测试：获取多个副本
func BenchmarkConsistentHashGetN(b *testing.B) {
	ch := NewConsistentHash(150)
	
	// 添加节点
	for i := 0; i < 10; i++ {
		ch.Add(fmt.Sprintf("node%d", i))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		ch.GetN(key, 3)
	}
}
