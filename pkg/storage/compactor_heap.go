package storage

import (
	"bytes"
	"container/heap"

	"github.com/ph0m1/kv/pkg/storage/sstable"
)

// 存储在最小堆中的元素
type heapItem struct {
	key   []byte
	value []byte

	isTombstone bool

	// 这个 item 来自哪个迭代器 (即哪个SSTable文件)
	// 这个 "索引" 非常关键，用于实现 "最新版本优先"
	iteratorIndex int
}

// compactionHeap : 最小堆的 heap.Interface 实现
// 按照 key 排序， key相同的情况下按照 iteratorIndex 降序排序
type compactionHeap struct {
	items     []heapItem
	iterators []sstable.Iterator
}

// 创建并初始化一个压缩堆

func newCompactionHeap(iters []sstable.Iterator) *compactionHeap {
	h := &compactionHeap{
		items:     make([]heapItem, 0),
		iterators: iters,
	}

	for i, iter := range iters {
		if iter.Next() {
			h.items = append(h.items, heapItem{
				key:           iter.Key(),
				value:         iter.Value(),
				isTombstone:   iter.IsTombstone(),
				iteratorIndex: i,
			})
		}
	}
	heap.Init(h)
	return h
}

func (h *compactionHeap) Len() int { return len(h.items) }

func (h *compactionHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h.items[i].key, h.items[j].key)
	if cmp < 0 {
		return true // key i < key j
	}
	if cmp > 0 {
		return false // key i > key j
	}

	// !! 关键的去重逻辑 !!
	// 如果 key 相同 (cmp == 0)
	// 我们希望 "iteratorIndex" 更大 (即更新的文件) 的那个 "更小" (优先级更高)
	//
	// 在 L0 -> L1 压缩中，InputFiles 列表 [0, 1, 2, 3]
	// 索引 3 是最新的文件 (newest)
	// 索引 0 是最旧的文件 (oldest)
	//
	// 所以，我们返回 h.items[i].iteratorIndex > h.items[j].iteratorIndex
	return h.items[i].iteratorIndex > h.items[j].iteratorIndex
}

func (h *compactionHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *compactionHeap) Push(x any) {
	h.items = append(h.items, x.(heapItem))
}

func (h *compactionHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1] // 弹出最后一个
	h.items = old[0 : n-1]
	return item
}

// --- 堆的辅助方法 ---

// PopAndRefill 弹出堆顶（最小）元素，
// 并从该元素所属的迭代器中补充下一个元素
func (h *compactionHeap) PopAndRefill() (heapItem, bool) {
	if h.Len() == 0 {
		return heapItem{}, false // 堆已空
	}

	// 1. 弹出最小元素
	item := heap.Pop(h).(heapItem)

	// 2. 找到它来自的迭代器
	iter := h.iterators[item.iteratorIndex]

	// 3. 从该迭代器中补充下一个元素
	if iter.Next() {
		heap.Push(h, heapItem{
			key:           iter.Key(),
			value:         iter.Value(),
			isTombstone:   iter.IsTombstone(),
			iteratorIndex: item.iteratorIndex,
		})
	}

	return item, true
}

// Peek 返回堆顶元素，但不弹出
func (h *compactionHeap) Peek() (heapItem, bool) {
	if h.Len() == 0 {
		return heapItem{}, false
	}
	return h.items[0], true
}
