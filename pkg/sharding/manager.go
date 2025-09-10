package sharding

import (
	"context"
	"fmt"
	"sync"

	"github.com/ph0m1/kv/pkg/client"
	"github.com/ph0m1/kv/pkg/storage"
)

// Node 表示集群中的一个节点
type Node struct {
	ID      string // 节点ID
	Address string // 节点地址 (host:port)
	Client  *client.Client
}

// Manager 分片管理器
type Manager struct {
	mu sync.RWMutex
	
	// 一致性哈希环
	hashRing *ConsistentHash
	
	// 节点信息
	nodes map[string]*Node
	
	// 本地节点ID
	localNodeID string
	
	// 本地存储引擎
	localStorage storage.Storage
	
	// 副本数量
	replicationFactor int
}

// NewManager 创建分片管理器
func NewManager(localNodeID string, localStorage storage.Storage, replicationFactor int, virtualNodes int) *Manager {
	if replicationFactor <= 0 {
		replicationFactor = 3 // 默认3副本
	}
	
	if virtualNodes <= 0 {
		virtualNodes = 150 // 默认150个虚拟节点
	}
	
	return &Manager{
		hashRing:          NewConsistentHash(virtualNodes),
		nodes:             make(map[string]*Node),
		localNodeID:       localNodeID,
		localStorage:      localStorage,
		replicationFactor: replicationFactor,
	}
}

// AddNode 添加节点
func (m *Manager) AddNode(nodeID, address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	
	if _, exists := m.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists", nodeID)
	}
	
	// 添加到哈希环
	m.hashRing.Add(nodeID)
	
	// 创建节点信息
	node := &Node{
		ID:      nodeID,
		Address: address,
	}
	
	// 如果不是本地节点，创建客户端连接
	if nodeID != m.localNodeID && address != "" {
		cli, err := client.NewClient(address, nil)
		if err != nil {
			// 移除已添加的哈希环节点
			m.hashRing.Remove(nodeID)
			return fmt.Errorf("failed to create client for node %s: %w", nodeID, err)
		}
		node.Client = cli
	}
	
	m.nodes[nodeID] = node
	
	return nil
}

// RemoveNode 移除节点
func (m *Manager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s does not exist", nodeID)
	}
	
	// 关闭客户端连接
	if node.Client != nil {
		node.Client.Close()
	}
	
	// 从哈希环移除
	m.hashRing.Remove(nodeID)
	
	delete(m.nodes, nodeID)
	
	return nil
}

// GetNode 根据键获取主节点
func (m *Manager) GetNode(key string) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	nodeID := m.hashRing.Get(key)
	if nodeID == "" {
		return nil, fmt.Errorf("no nodes available")
	}
	
	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	
	return node, nil
}

// GetNodes 根据键获取副本节点列表
func (m *Manager) GetNodes(key string) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	nodeIDs := m.hashRing.GetN(key, m.replicationFactor)
	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}
	
	nodes := make([]*Node, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node, exists := m.nodes[nodeID]
		if !exists {
			return nil, fmt.Errorf("node %s not found", nodeID)
		}
		nodes = append(nodes, node)
	}
	
	return nodes, nil
}

// IsLocalKey 判断键是否属于本地节点
func (m *Manager) IsLocalKey(key string) (bool, error) {
	node, err := m.GetNode(key)
	if err != nil {
		return false, err
	}
	
	return node.ID == m.localNodeID, nil
}

// Get 路由Get请求
func (m *Manager) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("key cannot be empty")
	}
	
	// 判断键是否属于本地节点
	isLocal, err := m.IsLocalKey(string(key))
	if err != nil {
		return nil, false, fmt.Errorf("failed to determine key location: %w", err)
	}
	
	if isLocal {
		// 本地读取
		return m.localStorage.Get(key)
	}
	
	// 转发到远程节点
	node, err := m.GetNode(string(key))
	if err != nil {
		return nil, false, fmt.Errorf("failed to get node for key: %w", err)
	}
	
	if node.Client == nil {
		return nil, false, fmt.Errorf("no client available for node %s", node.ID)
	}
	
	return node.Client.Get(ctx, key)
}

// Put 路由Put请求
func (m *Manager) Put(ctx context.Context, key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	
	// 判断键是否属于本地节点
	isLocal, err := m.IsLocalKey(string(key))
	if err != nil {
		return fmt.Errorf("failed to determine key location: %w", err)
	}
	
	if isLocal {
		// 本地写入
		return m.localStorage.Put(key, value)
	}
	
	// 转发到远程节点
	node, err := m.GetNode(string(key))
	if err != nil {
		return fmt.Errorf("failed to get node for key: %w", err)
	}
	
	if node.Client == nil {
		return fmt.Errorf("no client available for node %s", node.ID)
	}
	
	return node.Client.Put(ctx, key, value)
}

// Delete 路由Delete请求
func (m *Manager) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	
	// 判断键是否属于本地节点
	isLocal, err := m.IsLocalKey(string(key))
	if err != nil {
		return fmt.Errorf("failed to determine key location: %w", err)
	}
	
	if isLocal {
		// 本地删除
		return m.localStorage.Delete(key)
	}
	
	// 转发到远程节点
	node, err := m.GetNode(string(key))
	if err != nil {
		return fmt.Errorf("failed to get node for key: %w", err)
	}
	
	if node.Client == nil {
		return fmt.Errorf("no client available for node %s", node.ID)
	}
	
	return node.Client.Delete(ctx, key)
}

// GetAllNodes 获取所有节点
func (m *Manager) GetAllNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	
	return nodes
}

// NodeCount 返回节点数量
func (m *Manager) NodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return len(m.nodes)
}

// Close 关闭所有客户端连接
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	for _, node := range m.nodes {
		if node.Client != nil {
			node.Client.Close()
		}
	}
	
	return nil
}
