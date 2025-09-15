package replication

import (
	"encoding/binary"
	"fmt"
)

// CommandType 命令类型
type CommandType byte

const (
	CommandPut CommandType = iota
	CommandDelete
)

// Command 表示一个状态机命令
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
}

// EncodeCommand 编码命令
func EncodeCommand(cmdType CommandType, key, value []byte) []byte {
	// 格式: [Type:1][KeyLen:4][Key:N][ValueLen:4][Value:M]
	
	keyLen := len(key)
	valueLen := len(value)
	
	buf := make([]byte, 1+4+keyLen+4+valueLen)
	
	// Type
	buf[0] = byte(cmdType)
	
	// KeyLen
	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	
	// Key
	copy(buf[5:5+keyLen], key)
	
	// ValueLen
	binary.BigEndian.PutUint32(buf[5+keyLen:9+keyLen], uint32(valueLen))
	
	// Value
	copy(buf[9+keyLen:], value)
	
	return buf
}

// DecodeCommand 解码命令
func DecodeCommand(data []byte) (*Command, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("invalid command data: too short")
	}
	
	// Type
	cmdType := CommandType(data[0])
	
	// KeyLen
	keyLen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < int(5+keyLen+4) {
		return nil, fmt.Errorf("invalid command data: key length mismatch")
	}
	
	// Key
	key := make([]byte, keyLen)
	copy(key, data[5:5+keyLen])
	
	// ValueLen
	valueLen := binary.BigEndian.Uint32(data[5+keyLen : 9+keyLen])
	if len(data) < int(9+keyLen+valueLen) {
		return nil, fmt.Errorf("invalid command data: value length mismatch")
	}
	
	// Value
	value := make([]byte, valueLen)
	copy(value, data[9+keyLen:9+keyLen+valueLen])
	
	return &Command{
		Type:  cmdType,
		Key:   key,
		Value: value,
	}, nil
}
