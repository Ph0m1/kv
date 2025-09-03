package storage

import (
	"bytes"
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
	maxLevels           = 7
	l0CompactionTrigger = 4 // 当 L0 的文件达到 4 个时，触发 L0 -> L1 压缩
)

// 一个压缩任务
type CompactionTask struct {
	Level       int // 输入层
	OutputLevel int // 输出层

	InputFiles []*sstable.Reader // 需要被合并的 SSTable
}

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

// levelSnapshot 提供了 Get() 操作所需的所有SSTable的
// 一个一致的、时间点(point-in-time)的视图
type levelSnapshot struct {
	levels [][]*sstable.Reader
}

// Close 释放快照持有的所有 Reader
func (s *levelSnapshot) Close() {
	// do nothing
}

// GetSnapshot 获取所有层级的原子快照
func (lm *LevelManager) GetSnapshot() *levelSnapshot {
	lm.lock.RLock() // <-- 获取读锁
	defer lm.lock.RUnlock()

	snap := &levelSnapshot{
		levels: make([][]*sstable.Reader, maxLevels),
	}

	for level, readers := range lm.levels {
		// 必须返回一个 *副本*，以防止 Compactor 修改
		// 正在被 Get() 使用的切片。
		snap.levels[level] = make([]*sstable.Reader, len(readers))
		copy(snap.levels[level], readers)
	}

	return snap
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

func (lm *LevelManager) FindCompactionTask() *CompactionTask {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	// 策略 1: L0 -> L1 压缩
	// 这是最高优先级的压缩
	if len(lm.levels[0]) >= l0CompactionTrigger {
		// 选择 L0 的所有文件
		l0Files := make([]*sstable.Reader, len(lm.levels[0]))
		copy(l0Files, lm.levels[0])

		// (TODO: 还需要选择 L1 中与 L0 键范围重叠的文件)
		// (目前 L1 为空，我们暂时简化)

		return &CompactionTask{
			Level:       0,
			OutputLevel: 1,
			InputFiles:  l0Files,
		}
	}

	// 策略 2: L_N -> L_N+1 压缩 (级联压缩)
	// (TODO: 检查 L1+, L2+ ... 的大小，看是否需要压缩到下一层)

	// 没有任务
	return nil
}

// 临时函数 (仅供 Phase A 测试)
// 在 Phase B 中, ApplyCompactionResult 将会取代这个
func (lm *LevelManager) RemoveTempCompactionFiles(task *CompactionTask) {
	if task.Level == 0 {
		lm.lock.Lock()
		// 简单地清空 L0
		// 警告: 这是一个不安全的操作，仅用于防止死循环
		lm.levels[0] = make([]*sstable.Reader, 0)
		lm.lock.Unlock()
	}
}

func (lm *LevelManager) ApplyCompactionResult(task *CompactionTask, newReaders []*sstable.Reader) ([]*sstable.Reader, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	// --- 验证 ---
	// 确保 task.InputFiles 仍然存在与层级中，防止重复应用

	// --- 移除旧文件 (InputFiles) ---
	// 我们需要一个辅助函数来从 slice 中移除元素

	// 从 L0 移除
	if task.Level == 0 {
		// L0 的输入文件总是 *所有* L0 文件
		lm.levels[0] = make([]*sstable.Reader, 0)
	} else {
		// L_N (N > 0)
		lm.levels[task.Level] = removeReaders(lm.levels[task.Level], task.InputFiles)
	}

	// (如果 L0->L1 压缩还涉及 L1 的文件，也需要从 L1 移除)
	// (我们目前的 FindCompactionTask 简化了这一点，所以暂时跳过)

	// --- 3. 添加新文件 (newReaders) ---
	lm.levels[task.OutputLevel] = append(lm.levels[task.OutputLevel], newReaders...)

	// --- 4. (重要！) 保持 L1+ 层的有序性 ---
	// L1+ 层的文件必须按 Key 排序，以便 Get() 可以二分查找
	if task.OutputLevel > 0 {
		sort.Slice(lm.levels[task.OutputLevel], func(i, j int) bool {
			// 按照 minKey 升序排序
			return bytes.Compare(
				lm.levels[task.OutputLevel][i].MinKey(),
				lm.levels[task.OutputLevel][j].MinKey(),
			) < 0
		})
	}

	// --- 5. 返回需要被物理删除的旧文件 ---
	return task.InputFiles, nil
}

// (在文件末尾) 添加一个辅助函数：
// removeReaders 从 'a' 中移除 'b' 中的所有元素
func removeReaders(a []*sstable.Reader, b []*sstable.Reader) []*sstable.Reader {
	// 创建一个 map 用于快速查找 'b' 中的元素
	removeSet := make(map[*sstable.Reader]bool)
	for _, r := range b {
		removeSet[r] = true
	}

	// 创建一个新的 slice，只包含不在 'b' 中的元素
	newSlice := make([]*sstable.Reader, 0, len(a))
	for _, r := range a {
		if !removeSet[r] {
			newSlice = append(newSlice, r)
		}
	}
	return newSlice
}
