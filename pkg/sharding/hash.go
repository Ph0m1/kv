package sharding

import (
	"hash/crc32"
	"sort"
	"sync"
)

// ConsistentHash 一致性哈希环
type ConsistentHash struct {
	mu           sync.RWMutex
	replicas     int               // 虚拟节点数量
	keys         []int             // 排序的哈希值
	hashMap      map[int]string    // 哈希值到节点的映射
	nodes        map[string]bool   // 节点集合
}

// NewConsistentHash 创建一致性哈希环
// replicas: 每个物理节点对应的虚拟节点数量
func NewConsistentHash(replicas int) *ConsistentHash {
	if replicas <= 0 {
		replicas = 150 // 默认150个虚拟节点
	}
	
	return &ConsistentHash{
		replicas: replicas,
		keys:     []int{},
		hashMap:  make(map[int]string),
		nodes:    make(map[string]bool),
	}
}

// Add 添加节点
func (ch *ConsistentHash) Add(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	
	if ch.nodes[node] {
		return // 节点已存在
	}
	
	ch.nodes[node] = true
	
	// 为每个物理节点创建多个虚拟节点
	for i := 0; i < ch.replicas; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(node + "#" + string(rune(i)))))
		ch.keys = append(ch.keys, hash)
		ch.hashMap[hash] = node
	}
	
	// 重新排序
	sort.Ints(ch.keys)
}

// Remove 移除节点
func (ch *ConsistentHash) Remove(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	
	if !ch.nodes[node] {
		return // 节点不存在
	}
	
	delete(ch.nodes, node)
	
	// 移除该节点的所有虚拟节点
	newKeys := []int{}
	for _, hash := range ch.keys {
		if ch.hashMap[hash] != node {
			newKeys = append(newKeys, hash)
		} else {
			delete(ch.hashMap, hash)
		}
	}
	
	ch.keys = newKeys
}

// Get 获取键对应的节点
func (ch *ConsistentHash) Get(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	
	if len(ch.keys) == 0 {
		return ""
	}
	
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	
	// 二分查找第一个大于等于hash的位置
	idx := sort.Search(len(ch.keys), func(i int) bool {
		return ch.keys[i] >= hash
	})
	
	// 如果超出范围，回到起点
	if idx == len(ch.keys) {
		idx = 0
	}
	
	return ch.hashMap[ch.keys[idx]]
}

// GetN 获取键对应的N个节点（用于副本）
func (ch *ConsistentHash) GetN(key string, n int) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	
	if len(ch.keys) == 0 || n <= 0 {
		return []string{}
	}
	
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	
	// 二分查找起始位置
	idx := sort.Search(len(ch.keys), func(i int) bool {
		return ch.keys[i] >= hash
	})
	
	if idx == len(ch.keys) {
		idx = 0
	}
	
	// 收集N个不同的物理节点
	result := []string{}
	seen := make(map[string]bool)
	
	for i := 0; i < len(ch.keys) && len(result) < n; i++ {
		pos := (idx + i) % len(ch.keys)
		node := ch.hashMap[ch.keys[pos]]
		
		if !seen[node] {
			result = append(result, node)
			seen[node] = true
		}
	}
	
	return result
}

// Nodes 返回所有节点
func (ch *ConsistentHash) Nodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	
	nodes := make([]string, 0, len(ch.nodes))
	for node := range ch.nodes {
		nodes = append(nodes, node)
	}
	
	sort.Strings(nodes)
	return nodes
}

// Count 返回节点数量
func (ch *ConsistentHash) Count() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	
	return len(ch.nodes)
}
