package storage

// Storage 定义存储引擎接口
type Storage interface {
	// Get 获取键对应的值
	// 返回: value, found, error
	Get(key []byte) ([]byte, bool, error)

	// Put 写入键值对
	Put(key, value []byte) error

	// Delete 删除键（实际是写入墓碑标记）
	Delete(key []byte) error

	// Close 关闭存储引擎
	Close() error
}

// Iterator 定义迭代器接口
type Iterator interface {
	// Next 移动到下一个元素
	Next() bool

	// Key 返回当前键
	Key() []byte

	// Value 返回当前值
	Value() []byte

	// Error 返回迭代过程中的错误
	Error() error

	// Close 关闭迭代器
	Close() error
}

// Snapshot 定义快照接口
type Snapshot interface {
	// Get 从快照中获取值
	Get(key []byte) ([]byte, bool, error)

	// NewIterator 创建快照迭代器
	NewIterator() Iterator

	// Release 释放快照
	Release()
}
