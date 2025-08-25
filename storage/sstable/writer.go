// +-----------------------------------+
// |         [Data Block 1]            |  <-- 存储 (key, value) 对
// |         [Data Block 2]            |
// |         [Data Block 3]            |
// |              ...                  |
// +-----------------------------------+
// |          [Index Block]            |  <-- 稀疏索引 (Key -> Data Block 的偏移量)
// +-----------------------------------+
// |            [Footer]               |  <-- "元数据"，指向 Index Block
// +-----------------------------------+

package sstable

import (
	"bufio"
	"encoding/binary"
	"os"
)

const (
	// Date Block 的目标大小 （4kb）
	dataBlockTargetSize = 4 * 1024
	// SSTable 文件末尾的魔数
	sstableMagic = 0xDEC0DE
	// Footer 的固定大小（Magic(8B) + IndexOffset(8B) + IndexLen(8B) = 24B
	footerSize = 24
)

// indexEntry 存储中内存中， 最后写入 Index Block
type indexEntry struct {
	lastKey []byte // 此 Block 中最后的 key
	offset  int64  // block 在文件中的起始偏移量
	length  int    // block 的长度
}

// Writer 负责构建 SSTable
type Writer struct {
	file   *bufio.Writer
	f      *os.File // 底层文件句柄
	offset int64    // 当前文件偏移量

	indexEntries []indexEntry // 内存中的索引条目

	// 用于构建当前的 Data Block
	currentBlockBuffer []byte
	currentBlockKeys   int
	blockLastKey       []byte
}

// NewWriter 创建一个新的 SSTable writer
func NewWriter(filePath string) (*Writer, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:               bufio.NewWriter(f),
		f:                  f,
		offset:             0,
		indexEntries:       make([]indexEntry, 0),
		currentBlockBuffer: make([]byte, 0, dataBlockTargetSize),
	}, nil
}

// CurrentSize 返回 Writer 当前写入的总字节数
func (w *Writer) CurrentSize() int64 {
	// offset 字段就是我们正在跟踪的文件总大小
	return w.offset
}

// Write (key, value, isTombstone)
// 这是我们需要定义的数据在 Data Block 中的格式 (Entry Format)
// 简单格式: [Tombstone(1B)] [KeyLen(4B)] [ValLen(4B)] [Key] [Value]
func (w *Writer) writeEntry(key, value []byte, isTombstone bool) {
	// entry buf
	keyLen := len(key)
	valLen := len(value)
	entrySize := 1 + 4 + 4 + keyLen + valLen
	entryBuf := make([]byte, entrySize)

	offset := 0

	// Tombstone
	if isTombstone {
		entryBuf[offset] = 1
	} else {
		entryBuf[offset] = 0
	}
	offset += 1

	// KeyLen
	binary.LittleEndian.PutUint32(entryBuf[offset:], uint32(keyLen))
	offset += 4

	// ValLen
	binary.LittleEndian.PutUint32(entryBuf[offset:], uint32(valLen))
	offset += 4

	// Key
	copy(entryBuf[offset:], key)
	offset += keyLen

	// Value
	copy(entryBuf[offset:], value)

	// 写入当前 block buf
	w.currentBlockBuffer = append(w.currentBlockBuffer, entryBuf...)
	w.currentBlockKeys += 1
	w.blockLastKey = key
}

// Writer
func (w *Writer) Write(key, value []byte, isTombstone bool) {
	w.writeEntry(key, value, isTombstone)
}

// Flush 迭代器
// 从 Memtable 迭代器中读取所有数据构建 SSTable
func (w *Writer) Flush(iter Iterator) error {
	for iter.Next() {
		// 将 KV 写如当前 Data Block buf
		w.writeEntry(iter.Key(), iter.Value(), iter.IsTombstone())

		// 检查 Data Block 大小
		if len(w.currentBlockBuffer) >= dataBlockTargetSize {
			if err := w.flushCurrentDataBlock(); err != nil {
				return err
			}
		}
	}

	// 确保最后一个（可能未满的）Data Block 也被刷写
	if w.currentBlockKeys > 0 {
		if err := w.flushCurrentDataBlock(); err != nil {
			return err
		}
	}

	// 所有 Data Block 都写完了，开始写 Index Block
	indexOffset, indexLen, err := w.writeIndexBlock()
	if err != nil {
		return err
	}

	// 最后写入 Footer
	if err := w.writeFooter(indexOffset, indexLen); err != nil {
		return err
	}

	// 刷盘
	return w.Close()

}

// flushCurrentDataBlock 将内存中的 Data Block 写入文件
func (w *Writer) flushCurrentDataBlock() error {
	if w.currentBlockKeys == 0 {
		return nil // 没有数据
	}

	// 写入文件
	n, err := w.file.Write(w.currentBlockBuffer)
	if err != nil {
		return err
	}

	// 创建索引条目
	entry := indexEntry{
		lastKey: w.blockLastKey,
		offset:  w.offset,
		length:  n,
	}
	w.indexEntries = append(w.indexEntries, entry)

	// 更新文件偏移量
	w.offset += int64(n)

	// 重置 Data Block buf
	w.currentBlockBuffer = w.currentBlockBuffer[:0]
	w.currentBlockKeys = 0
	w.blockLastKey = nil

	return nil
}

// writeIndexBlock 将内存中的索引条目序列化并写入文件
func (w *Writer) writeIndexBlock() (indexOffset int64, indexLen int, err error) {
	indexOffset = w.offset

	// Index Block 格式:
	// [EntryCount (4B)]
	// [Entry1_KeyLen (4B)] [Entry1_Key] [Entry1_Offset (8B)] [Entry1_Len (4B)]
	// [Entry2_KeyLen (4B)] [Entry2_Key] [Entry2_Offset (8B)] [Entry2_Len (4B)]
	// ...

	// 写入 EntryCount
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(w.indexEntries)))
	if _, err := w.file.Write(countBuf); err != nil {
		return 0, 0, err
	}
	indexLen += 4

	for _, entry := range w.indexEntries {
		// KeyLen
		keyLenBuf := make([]byte, 4)
		keyLen := len(entry.lastKey)
		binary.LittleEndian.PutUint32(keyLenBuf, uint32(keyLen))
		if _, err := w.file.Write(keyLenBuf); err != nil {
			return 0, 0, err
		}

		// Key
		if _, err := w.file.Write(entry.lastKey); err != nil {
			return 0, 0, err
		}

		// Offset
		offsetBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBuf, uint64(entry.offset))
		if _, err := w.file.Write(offsetBuf); err != nil {
			return 0, 0, err
		}

		// Lenhth
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(entry.length))
		if _, err := w.file.Write(lenBuf); err != nil {
			return 0, 0, err
		}

		indexLen += (4 + keyLen + 8 + 4)
	}
	w.offset += int64(indexLen)
	return indexOffset, indexLen, nil
}

func (w *Writer) writeFooter(indexOffset int64, indexLen int) error {
	footerBuf := make([]byte, footerSize)

	// 8B IndexOffset
	binary.LittleEndian.PutUint64(footerBuf[0:8], uint64(indexOffset))
	// 8B IndexLength
	binary.LittleEndian.PutUint64(footerBuf[8:16], uint64(indexLen))
	// 8B 字节 MagicNumber
	binary.LittleEndian.PutUint64(footerBuf[16:24], uint64(sstableMagic))

	if _, err := w.file.Write(footerBuf); err != nil {
		return err
	}
	w.offset += footerSize
	return nil
}

// Close 刷写缓冲区并关闭文件
func (w *Writer) Close() error {
	if err := w.file.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}
