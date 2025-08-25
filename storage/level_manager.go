package storage

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ph0m1/kv/storage/sstable"
)

const (
	maxLevels = 7
)

// LevelManager 负责跟踪所有层级的 SSTable
type LevelManager struct {
	lock sync.RWMutex

	// levels[0] 是 L0
	// levels[1...N] 是 L1...
	levels [][]*sstable.Reader

	dirPath string

	// 文件序号生成器，取代 lsm.go 中的 buggy 逻辑
	nextFileNum int
}

// NewLevelManager 创建或加载层级管理器
func NewLevelManager(dirPath string) (*LevelManager, error) {
	lm := &LevelManager{
		levels:      make([][]*sstable.Reader, maxLevels),
		dirPath:     dirPath,
		nextFileNum: 1, // 总是从 1 开始
	}

	for i := 0; i < maxLevels; i++ {
		lm.levels[i] = make([]*sstable.Reader, 0)
	}

	// 加载所有现有的 .sst 文件
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data dir: %w", err)
	}

	var sstFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			sstFiles = append(sstFiles, f.Name())
		}
	}
	sort.Strings(sstFiles) // 确保顺序

	for _, fileName := range sstFiles {
		sstPath := filepath.Join(dirPath, fileName)
		reader, err := sstable.NewReader(sstPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load sstable %s: %w", fileName, err)
		}

		// 重点：我们如何知道它属于哪一层？
		// 缺乏 "MANIFEST" 文件，我们只能做一个*简化*：
		// 暂时将所有文件都加载到 L0
		// (这是一个临时措施，压缩器稍后会修复它)
		lm.levels[0] = append(lm.levels[0], reader)

		// 更新文件序号
		num, _ := strconv.Atoi(strings.TrimSuffix(fileName, ".sst"))
		if num >= lm.nextFileNum {
			lm.nextFileNum = num + 1
		}
	}
	return lm, nil
}

// AddL0 将一个新刷写的 SSTable 添加到 L0
func (lm *LevelManager) AddL0(reader *sstable.Reader) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	// L0 的新文件总是在末尾 (最新的)
	lm.levels[0] = append(lm.levels[0], reader)
}

// NextFileNumber 返回一个唯一的 SSTable 文件编号
func (lm *LevelManager) NextFileNumber() int {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	num := lm.nextFileNum
	lm.nextFileNum += 1
	return num
}

// GetReadView 返回一个用于读取的“快照”
// 这是一个关键的并发控制点
// (目前实现比较简单，只是返回 L0)
func (lm *LevelManager) GetL0Readers() []*sstable.Reader {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	// 必须复制一份切片，防止在 Get 查询时
	// Compactor 修改了底层的 l0 列表
	readers := make([]*sstable.Reader, len(lm.levels[0]))
	copy(readers, lm.levels[0])
	return readers
}
