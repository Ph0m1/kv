package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// wal.go
// 实现了 Write-Ahead Log (WAL)
//
// WAL Entry (日志条目) 磁盘格式:
// +------------------+-------------------+--------------------+-------------------+----------------+---------------+
// | Checksum (4 bytes) | Tombstone (1 byte) | Key Length (4 bytes) | Val Length (4 bytes) | Key (variable) | Value (variable) |
// +------------------+-------------------+--------------------+-------------------+----------------+---------------+
//
// Checksum: CRC32 of (Tombstone + KeyLen + ValLen + Key + Value)
// Tombstone: 1 byte (1 for delete, 0 for put)
// Key Length: uint32 (4 bytes)
// Val Length: uint32 (4 bytes)

const (
	// entryHeaderSize = Checksum (4) + Tombstone (1) + KeyLen (4) + ValLen (4)
	entryHeaderSize = 13
)

// walEntry 在内存中表示一个内存条目
type walEntry struct {
	key        []byte
	value      []byte
	isTomstone bool
}

// WAL
type WAL struct {
	file *os.File
	lock sync.Mutex
	buf  []byte
}

// NewWAL 打开或创建一个 WAL 文件
func NewWAL(filepath string) (*WAL, error) {
	// os.O_CREATE: 文件不存在则创建
	// os.O_APPEND: 写入时总是在文件末尾追加
	// os.O_RDWR: 读写模式 (Replay 时需要读)
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		buf:  make([]byte, entryHeaderSize), // 至少分配 header 大小
	}, nil
}

// encodeEntry 将 walEntry 序列化为 []byte
// 格式： Checksum (4) + Tombstone (1) + KeyLen (4) + ValLen(4) + Key + Value

func (e *walEntry) encode(buf []byte) []byte {
	keyLen := len(e.key)
	valLen := len(e.key)
	totalLen := entryHeaderSize + keyLen + valLen

	// 足够大
	if len(buf) < totalLen {
		buf = make([]byte, totalLen)
	}

	// 写入 Tombstone (1 byte)
	offset := 4 // 跳过 Checksum
	if e.isTomstone {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset += 1 // 下一位: KeyLen

	binary.LittleEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4 // valLen

	binary.LittleEndian.PutUint32(buf[offset:], uint32(valLen))
	offset += 4 // key

	copy(buf[offset:], e.key)
	offset += keyLen // value

	copy(buf[offset:], e.value)

	// 计算 checksum 并写入
	checksum := crc32.ChecksumIEEE(buf[4:totalLen])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf[:totalLen]
}

// decodeEntry 从 reader 中解码一个entry
// 返回 entry， 读取的总字节数， 错误
func decodeEntry(r io.Reader) (*walEntry, int64, error) {
	header := make([]byte, entryHeaderSize)

	// 读取 header
	n, err := io.ReadFull(r, header)
	if err != nil {
		// io.EOF / io.ErrUnexpectedEOF 文件结束或损坏
		return nil, int64(n), err
	}

	savedChecksum := binary.LittleEndian.Uint32(header[0:4])

	entry := &walEntry{}
	offset := 4 // Tombstone

	if header[offset] == 1 {
		entry.isTomstone = true
	}
	offset += 1 // keyLen

	keyLen := binary.LittleEndian.Uint32(header[offset : offset+4])
	offset += 4

	valLen := binary.LittleEndian.Uint32(header[offset : offset+4])

	totalLen := int(keyLen + valLen)
	kvBuf := make([]byte, totalLen)
	n, err = io.ReadFull(r, kvBuf)
	if err != nil {
		return nil, int64(entryHeaderSize + n), err
	}

	// 校验 checksum
	currentCs := crc32.ChecksumIEEE(header[4:])
	currentCs = crc32.Update(currentCs, crc32.IEEETable, kvBuf)

	if savedChecksum != currentCs {
		return nil, int64(entryHeaderSize + n), io.ErrUnexpectedEOF // 文件损坏
	}

	entry.key = kvBuf[:keyLen]
	entry.value = kvBuf[keyLen:]

	return entry, int64(entryHeaderSize + totalLen), nil
}

// Write 将一个条目写入 WAL
// !! 必须确保数据落盘（fsync）!!
func (w *WAL) Write(key, value []byte, isTombstone bool) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	entry := &walEntry{
		key:        key,
		value:      value,
		isTomstone: isTombstone,
	}

	// 序列化
	w.buf = entry.encode(w.buf)

	// 写入文本
	if _, err := w.file.Write(w.buf); err != nil {
		return err
	}

	// 强制刷盘 阻塞
	return w.file.Sync()
}

// Replay WAL
// 遍历 WAL 文件, 为每个条目调用 replayFunc
func (w *WAL) Replay(replyFunc func(key, value []byte, isTombstone bool) error) error {
	// 确保 Replay 前所有待写入的都已写入
	w.lock.Lock()
	defer w.lock.Unlock()
	// 移到开头
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		entry, _, err := decodeEntry(w.file)
		if err != nil {
			if err == io.EOF { // 正常读到 EOF
				break
			}
			// 文件损坏或读取错误
			return err
		}
		// 回调， 将数据加载会 Memtable
		if err := replyFunc(entry.key, entry.value, entry.isTomstone); err != nil {
			// 回调失败停止 Replay
			return err
		}
	}
	// 移回末尾，便于 append
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

// CLose 关闭 WAL 文件
func (w *WAL) CLose() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.file.Close()
}

// Clear 关闭并删除 WAL 文件
// 在 Memtable 成功刷为 SSTable 后调用
func (w *WAL) Clear() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}
	return os.Remove(w.file.Name())
}
