package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Reader 负责从 SSTable 中读取数据
type Reader struct {
	f    *os.File
	lock sync.RWMutex // 允许多个 Get 并发读取

	// 从文件加载的稀疏索引
	// (key, block_offset, block_length)
	index []indexEntry

	// 文件的总大小，校验
	fileSize int64
	filePath string

	// 缓存文件的键范围
	minKey []byte
	maxKey []byte
}

// NewReader 打开一个 SSTable 文件 并加载索引
func NewReader(filePath string) (*Reader, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fileSize := fi.Size()

	// 读取 Footer
	footerBuf := make([]byte, footerSize)
	// 从文件末尾倒着读
	_, err = f.ReadAt(footerBuf, fileSize-footerSize)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read sstable footer: %w", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footerBuf[0:8])
	indexLen := binary.LittleEndian.Uint64(footerBuf[8:16])
	magic := binary.LittleEndian.Uint64(footerBuf[16:24])

	if magic != sstableMagic {
		f.Close()
		return nil, fmt.Errorf("invalid sstable magic number: %x", magic)
	}
	if indexOffset+indexLen+footerSize != uint64(fileSize) {
		f.Close()
		return nil, fmt.Errorf("sstable footer indicates invalid index or file size")
	}

	// 读取 Index Block
	indexBuf := make([]byte, indexLen)
	_, err = f.ReadAt(indexBuf, int64(indexOffset))
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read sstable index block: %w", err)
	}

	// 解析 Index Block 到内存中的 index []indexEntry
	var indexEntries []indexEntry
	offset := 0

	// 读取 EntryCount
	entryCount := binary.LittleEndian.Uint32(indexBuf[offset : offset+4])
	offset += 4

	for i := 0; i < int(entryCount); i++ {
		// KeyLen
		keyLen := binary.LittleEndian.Uint32(indexBuf[offset : offset+4])
		offset += 4

		// Key
		key := make([]byte, keyLen)
		copy(key, indexBuf[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Offset
		blockOffset := binary.LittleEndian.Uint64(indexBuf[offset : offset+8])
		offset += 8

		// Length
		blockLen := binary.LittleEndian.Uint32(indexBuf[offset : offset+4])
		offset += 4

		indexEntries = append(indexEntries, indexEntry{
			lastKey: key,
			offset:  int64(blockOffset),
			length:  int(blockLen),
		})
	}

	// 缓存 minKey 和 maxKey
	var minKey, maxKey []byte

	if len(indexEntries) > 0 {
		maxKey = indexEntries[len(indexEntries)-1].lastKey

		firstBlockEntry := indexEntries[0]

		// 创建一个临时 Reader
		tempReader := &Reader{f: f}
		firstBlockBuf, err := tempReader.readBlock(firstBlockEntry.offset, firstBlockEntry.length)

		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to read first block for minKwy: %w", err)
		}

		if len(firstBlockBuf) > 5 { // 1(tomb) + 4(keyLen)
			// 跳过 Tombstone (1)
			keyLen := binary.LittleEndian.Uint32(firstBlockBuf[1:5])
			if len(firstBlockBuf) >= int(9+keyLen) { // 1+4+4+keyLen
				// 跳过 1(tomb) + 4(keyLen) + 4(valLen)
				minKey = firstBlockBuf[9 : 9+keyLen]
			}
		}
	}

	return &Reader{
		f:        f,
		index:    indexEntries,
		fileSize: fileSize,
		filePath: filePath,
		minKey:   minKey,
		maxKey:   maxKey,
	}, nil
}

// NewIterator 创建一个 SSTable 迭代器
func (r *Reader) NewIterator() (*sstableIterator, error) {
	it := &sstableIterator{
		r:          r,
		blockIndex: -1, // 尚未开始
	}

	// 立刻加载第一个 block
	if err := it.loadBlock(0); err != nil {
		if err == io.EOF {
			return it, nil
		}
		return nil, err
	}
	return it, nil
}

// Get 查找一个键
// 返回 (value, isTombstone, found)
func (r *Reader) Get(key []byte) ([]byte, bool, bool, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	// 在内存中的稀疏索引中进行二分查找，找到包含 key 的 Data Block
	// findBlockIndex 返回索引数组中第一个 lastKey >= key 的 Data Block 的索引
	blockIdx := r.findBlockIndex(key)
	if blockIdx == -1 {
		return nil, false, false, nil // 键超出了所有 Data Block 的范围
	}

	targetBlock := r.index[blockIdx]

	// 从磁盘读取整个 Data Block
	blockBuf, err := r.readBlock(targetBlock.offset, targetBlock.length) // <-- 修改
	if err != nil {
		return nil, false, false, err // <-- 修改
	}

	// 在 Data Block 内部进行线性扫描查找（也可以是二分查找，取决于实现复杂度）
	// 我们这里采用线性扫描，因为 Data Block 普遍较小 (例如 4KB)，线性扫描足够快且简单。
	// (更复杂的实现会用 Block 内的 Restart Point 进行二分查找)
	offset := 0
	for offset < len(blockBuf) {
		// Tombstone
		isTombstone := blockBuf[offset] == 1
		offset++

		// KeyLen
		keyLen := binary.LittleEndian.Uint32(blockBuf[offset : offset+4])
		offset += 4

		// ValLen
		valLen := binary.LittleEndian.Uint32(blockBuf[offset : offset+4])
		offset += 4

		// Key
		currentKey := blockBuf[offset : offset+int(keyLen)]
		offset += int(keyLen)

		// Value
		currentValue := blockBuf[offset : offset+int(valLen)]
		offset += int(valLen)

		cmp := bytes.Compare(currentKey, key)
		if cmp == 0 {
			// 找到精确匹配
			return currentValue, isTombstone, true, nil
		} else if cmp > 0 {
			// 因为 Data Block 是有序的，如果当前 key 已经比目标 key 大了，
			// 那么后面的 key 就更大了，肯定找不到
			return nil, false, false, nil
		}
		// 继续下一条
	}

	return nil, false, false, nil
}

// findBlockIndex 在稀疏索引中查找包含 key 的 Data Block 的索引
// 返回索引，或 -1 如果 key 小于所有 lastKey
func (r *Reader) findBlockIndex(key []byte) int {
	// 使用 Go 标准库的 sort.Search 辅助进行二分查找
	// 查找第一个满足 f(i) 为 true 的索引 i
	// 这里 f(i) 是 r.index[i].lastKey >= key
	idx := search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].lastKey, key) >= 0
	})

	if idx < len(r.index) {
		return idx
	}
	return -1 // 未找到，Key 大于所有 Data Block 的 lastKey
}

// search 是 sort.Search 的一个简化实现，用于 bytes.Compare
// 查找第一个 f(i) 为 true 的索引
func search(n int, f func(int) bool) int {
	// 定义搜索区间 [i, j)
	i, j := 0, n
	for i < j {
		h := i + (j-i)/2 // 中间点
		if f(h) {
			j = h // 可能是 h，或者更小
		} else {
			i = h + 1 // 肯定不是 h，向右搜索
		}
	}
	return i
}

// readBlock 从磁盘读取一个完整的数据块
func (r *Reader) readBlock(offset int64, length int) ([]byte, error) {
	blockBuf := make([]byte, length)
	_, err := r.f.ReadAt(blockBuf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read data block: %w", err)
	}
	return blockBuf, nil
}

// MinKey 返回此 SSTable 中的最小键
func (r *Reader) MinKey() []byte {
	return r.minKey
}

// MaxKey 返回此 SSTable 中的最大键
func (r *Reader) MaxKey() []byte {
	return r.maxKey
}

// Close 关闭文件句柄
func (r *Reader) Close() error {
	r.lock.Lock() // 获取写锁，防止在关闭时有读操作
	defer r.lock.Unlock()
	return r.f.Close()
}

// 访问 filePath
func (r *Reader) FilePath() string {
	return r.filePath
}
