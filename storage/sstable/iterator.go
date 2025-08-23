package sstable

import (
	"encoding/binary"
	"io"
)

// 按顺序遍历 SSTable
type sstableIterator struct {
	r *Reader // 底层的 SSTable Reader

	blockIndex int            // 当前正在读取的 Data Block 的索引
	blockIter  *blockIterator // 当前 Data Block 的迭代器
}

// blockIterator 负责遍历单个 Data Block 内部的条目
// 内存中的迭代器
type blockIterator struct {
	data   []byte // 整个 Data Block 的内容
	offset int    // 当前在 block 内的偏移量

	// 当前条目的值
	key         []byte
	value       []byte
	isTombstone bool
}

// loadBlock 加载第 blockIdx 个 Data Block 并为其创建 blockIterator
func (it *sstableIterator) loadBlock(blockIdx int) error {
	it.r.lock.RLock()
	defer it.r.lock.RUnlock()

	if blockIdx >= len(it.r.index) {
		// 没有更多 Data Block
		it.blockIndex = blockIdx
		it.blockIter = nil // 标记结束
		return io.EOF
	}

	// 获取 Data Block 的位置
	targetBlock := it.r.index[blockIdx]

	// 从磁盘读
	blockBuf := make([]byte, targetBlock.length)
	_, err := it.r.f.ReadAt(blockBuf, targetBlock.offset)
	if err != nil {
		it.blockIter = nil
		return err
	}
	// 创建 Data Block 的迭代器
	it.blockIndex = blockIdx
	it.blockIter = newBlockIterator(blockBuf)
	return nil
}

// newBlockIterator 为一个 Data Block 缓冲区创建迭代器
func newBlockIterator(data []byte) *blockIterator {
	return &blockIterator{
		data:   data,
		offset: 0,
	}
}

// Next (Block Iterator) 移动到 Data Block 中的下一条
// 内存中
func (bi *blockIterator) Next() bool {
	if bi.offset >= len(bi.data) {
		return false
	}

	// Tombstion
	bi.isTombstone = bi.data[bi.offset] == 1
	bi.offset += 1

	// KeyLen
	keyLen := binary.LittleEndian.Uint32(bi.data[bi.offset : bi.offset+4])
	bi.offset += 4

	// valLen
	valLen := binary.LittleEndian.Uint32(bi.data[bi.offset : bi.offset+4])
	bi.offset += 4

	// Key
	bi.key = bi.data[bi.offset : bi.offset+int(keyLen)]
	bi.offset += int(keyLen)

	// Value
	bi.value = bi.data[bi.offset : bi.offset+int(valLen)]

	return true
}

// Next (SSTable Iterator) 移动到 SSTable 中的下一个条目
// 这是 Compactor 调用的主要方法
func (it *sstableIterator) Next() bool {
	if it.blockIter == nil {
		return false // 迭代完成
	}

	// 尝试在当前 Data Block 中移动
	if it.blockIter.Next() {
		return true // 找到下一个条目
	}

	// 尝试加载下一个 Data Block
	err := it.loadBlock(it.blockIndex + 1)
	if err != nil {
		// io.EOF 或其他错误， 都表示迭代结束
		it.blockIter = nil
		return false
	}

	// 成功加载了新 Block，并在新 Block 上调用 Next()
	// 新 Block 至少有一条记录， 否则 blockIterator 为 nil
	return it.blockIter.Next()
}

// Key 返回当前条目的 Key
func (it *sstableIterator) Key() []byte {
	if it.blockIter == nil {
		return nil
	}
	return it.blockIter.key
}

// Value 返回当前条目的 Value
func (it *sstableIterator) Value() []byte {
	if it.blockIter == nil {
		return nil
	}
	return it.blockIter.value
}

// IsTombstone 返回当前条目是否为墓碑
func (it *sstableIterator) IsTombstone() bool {
	if it.blockIter == nil {
		return false
	}
	return it.blockIter.isTombstone
}

// Close 释放迭代器资源 (目前无需操作，但保持良好习惯)
func (it *sstableIterator) Close() error {
	it.blockIter = nil
	it.r = nil
	return nil
}
