package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	randomLevel     = 0.25
	defaultMaxLevel = 18
)

type Node struct {
	key   []byte
	value []byte

	// 删除标记
	isTombstone bool

	// 指向下一个节点的指针的数组，不同深度的节点长度不同，[0]表示最下层
	// forward[0] 是指向 Level 0 层的下一个节点
	// forward[1] 是指向 Level 1 层的下一个节点
	// ...
	// 数组的长度 (len(forward)) 就是这个节点的高度 (level)
	forward []*Node
}

func newNode(key []byte, value []byte, level int, isTombstone bool) *Node {
	return &Node{
		key:         key,
		value:       value,
		isTombstone: isTombstone,
		// 创建一个高度为 level 的切片
		forward: make([]*Node, level),
	}
}

func (n *Node) Key() []byte {
	return n.key
}

func (n *Node) Value() []byte {
	return n.value
}

func (n *Node) IsTombstone() bool {
	return n.isTombstone
}

type SkipList struct {
	head *Node
	// 读写锁
	mu sync.RWMutex

	rand *rand.Rand

	maxLevel     int
	currentLevel int

	// 跟踪 Memtable 的大致内存占用(bytes)
	size atomic.Int64
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:         newNode(nil, nil, maxLevel, false),
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel:     maxLevel,
		currentLevel: 1,
		size:         atomic.Int64{},
	}
}

// 只在持有写锁(sl.mu.Lock())时调用
func (sl *SkipList) randomLevel() int {
	level := 1
	// 注意：调用者必须已经持有锁
	for sl.rand.Float64() < randomLevel && level < sl.maxLevel {
		level++
	}
	return level
}

// Get() 获取 key 的值
// 返回 value isTombstone found
func (sl *SkipList) Get(key []byte) ([]byte, bool, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.head

	// 从高层开始，逐层向下查找
	for i := sl.currentLevel - 1; i >= 0; i-- {
		// 向右移动
		// 下一个节点键值小于key
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}
	// current 是 level 0 上小于 key 的最后一个节点
	// 下一个节点为潜在目标
	current = current.forward[0]

	// check
	if current != nil && bytes.Compare(current.key, key) == 0 {
		// yes
		return current.value, current.isTombstone, true
	}

	return nil, false, false
}

// Put 插入或更新
func (sl *SkipList) Put(key []byte, value []byte, isTombstone bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// update i 为 level i 上的前驱节点
	update := make([]*Node, sl.maxLevel)
	current := sl.head

	// 寻找插入位置, 记录前驱节点
	for i := sl.currentLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// 键已存在
	if current != nil && bytes.Compare(current.key, key) == 0 {
		// 计算大小变化
		oldSize := int64(len(current.value))
		newSize := int64(len(value))
		sizeDelta := newSize - oldSize
		sl.size.Add(sizeDelta)

		current.value = value
		current.isTombstone = isTombstone
		return
	}

	// 键不存在
	newLevel := sl.randomLevel()

	if newLevel > sl.currentLevel {
		// 超出的新层级的前驱节点都是 head
		for i := sl.currentLevel; i < newLevel; i++ {
			update[i] = sl.head
		}
		// 更新当前最大高度
		sl.currentLevel = newLevel
	}

	n := newNode(key, value, newLevel, isTombstone)

	// 更新大小
	// 暂时忽略 Tombstone 和 指针
	entrySize := int64(len(key) + len(value))
	sl.size.Add(entrySize)

	// 插入节点，更新指针
	// 从 Level 0 到 newLevel-1
	for i := 0; i < newLevel; i++ {
		// 新节点的 forward[i] 指向它前驱 (update[i]) 原来的 forward[i]
		n.forward[i] = update[i].forward[i]
		// 前驱 (update[i]) 的 forward[i] 指向新节点
		update[i].forward[i] = n
	}
}

// ApproximateSize 返回 Memtable 占用的大致内存（字节）
func (sl *SkipList) ApproximateSize() int64 {
	return sl.size.Load()
}

// 迭代器
type SkipListIterator struct {
	current *Node
}

func (sl *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{
		current: sl.head,
	}
}

func (it *SkipListIterator) Next() bool {
	if it.current == nil {
		return false
	}
	// 后移
	it.current = it.current.forward[0]
	return it.current != nil
}

func (it *SkipListIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

func (it *SkipListIterator) Value() []byte {
	if it == nil {
		return nil
	}
	return it.current.value
}

func (it *SkipListIterator) IsTombstone() bool {
	if it.current == nil {
		return false
	}
	return it.current.isTombstone
}

func (it *SkipListIterator) Close() error {
	return nil
}
